"""Yeelock device."""

import asyncio
import hashlib
import hmac
import logging
import uuid

from bluetooth_data_tools import monotonic_time_coarse
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import time

from bleak.exc import BleakError
from bleak_retry_connector import (
    BleakClientWithServiceCache,
    BleakConnectionError,
    close_stale_connections_by_address,
    establish_connection,
)
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.match import ADDRESS
from homeassistant.const import CONF_API_KEY, CONF_MAC, CONF_MODEL, CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr

from .const import (
    ACTIVE_SCAN_BURST_SECONDS,
    ADVERTISEMENT_WAIT_TIMEOUT,
    BLE_SEMAPHORE_KEY,
    COMMAND_FINAL_STATE,
    COMMAND_TRANSITIONAL_STATE,
    CONF_AUTO_UNLOCK_LOW_BATTERY,
    CONF_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
    CONNECTABLE_ADVERTISEMENT_MAX_AGE,
    CONNECTION_MAX_ATTEMPTS,
    CONNECT_PHASE_TIMEOUT,
    CONNECT_TIMEOUT_COOLDOWN_SECONDS,
    DEFAULT_AUTO_UNLOCK_LOW_BATTERY,
    DEFAULT_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
    DOMAIN,
    LOCKER_KIND,
    LOCKER_FAILURE_COOLDOWN_SECONDS,
    LOCK_ADVERTISEMENT_WAIT_TIMEOUT,
    LOCK_COMMAND_RESULT_TIMEOUT,
    NOTIFICATION_WAIT_SECONDS,
    PRE_CONNECT_DELAY_SECONDS,
    SERVICE_DISCOVERY_RETRY_AD_TIMEOUT,
    UUID_COMMAND,
    UUID_NOTIFY,
)


_LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def _adapter_session(hass: HomeAssistant) -> AsyncIterator[None]:
    """Serialize Yeelock BLE access across all locks on one adapter."""
    adapter_lock: asyncio.Lock = hass.data[DOMAIN][BLE_SEMAPHORE_KEY]
    async with adapter_lock:
        yield


class YeelockDeviceEntity:
    """Entity class for the Yeelock devices."""

    _attr_has_entity_name = True

    def __init__(self, yeelock_device, hass: HomeAssistant):
        """Init entity with the device."""
        self.hass = hass
        self.device: Yeelock = yeelock_device
        self._attr_unique_id = f"{yeelock_device.mac}_{self.__class__.__name__}"
        self._last_action = None  # Track last requested action

    @property
    def device_info(self):
        """Shared device info information."""
        return {
            "identifiers": {(DOMAIN, self.device.mac)},
            "connections": {(dr.CONNECTION_BLUETOOTH, self.device.mac)},
            "name": self.device.name,
            "manufacturer": self.device.manufacturer,
            "model": self.device.model,
        }


class Yeelock:
    """Yeelock class."""

    def __init__(self, config: dict, hass: HomeAssistant) -> None:
        """Initialize device."""
        self._hass = hass
        self._device = None
        self._lock = None
        self._battery_sensor = None
        self._client = None
        self._connecting = False
        self._connect_lock = asyncio.Lock()
        self._connected = False
        self.mac = config.get(CONF_MAC)
        self.name = config.get(CONF_NAME)
        self.key = config.get(CONF_API_KEY)
        self.model = config.get(CONF_MODEL, None)
        self.manufacturer = "Yeelock"
        self.battery_level = None
        self._last_action = None
        self.auto_unlock_low_battery = config.get(
            CONF_AUTO_UNLOCK_LOW_BATTERY,
            DEFAULT_AUTO_UNLOCK_LOW_BATTERY,
        )
        self.auto_unlock_low_battery_threshold = config.get(
            CONF_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
            DEFAULT_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
        )
        self._auto_unlock_triggered = False
        self._command_state_waiter: asyncio.Future[str] | None = None
        self._locker_cooldown_until = 0.0
        self._active_op_kind: str | None = None

    def _resolve_command_state_waiter(self, new_state: str) -> None:
        """Complete a pending lock/unlock wait when a final state arrives."""
        if self._command_state_waiter is None or self._command_state_waiter.done():
            return
        if new_state in ("locked", "unlocked", "jammed"):
            self._command_state_waiter.set_result(new_state)

    async def _wait_for_command_result(self, kind: str) -> None:
        """Stay connected until the lock reports a final state."""
        final_state = COMMAND_FINAL_STATE[kind]
        transitional_state = COMMAND_TRANSITIONAL_STATE[kind]

        if self._lock is not None and self._lock._attr_state == final_state:
            return

        if self._command_state_waiter is None:
            loop = asyncio.get_running_loop()
            self._command_state_waiter = loop.create_future()

        try:
            result = await asyncio.wait_for(
                asyncio.shield(self._command_state_waiter),
                timeout=LOCK_COMMAND_RESULT_TIMEOUT,
            )
            if result == "jammed":
                raise BleakError(f"Lock {self.mac} reported jammed")
        except TimeoutError:
            current = self._lock._attr_state if self._lock is not None else None
            if current == transitional_state and self._lock is not None:
                _LOGGER.info(
                    "Lock %s reached %s but did not confirm %s; assuming success",
                    self.name or self.mac,
                    transitional_state,
                    final_state,
                )
                await self._lock._update_lock_state(final_state)
                return
            if current in (transitional_state, final_state) and self._lock is not None:
                await self._lock._update_lock_state("unknown")
            raise BleakError(
                f"Lock {self.mac} did not confirm {final_state} within "
                f"{LOCK_COMMAND_RESULT_TIMEOUT:.0f}s"
            ) from None
        finally:
            self._command_state_waiter = None

    async def disconnect(self):
        """Disconnect from the device."""
        await self._disconnect_client()

    def _on_disconnect(self, _client) -> None:
        """Handle unexpected disconnects from the lock."""
        self._client = None
        self._connected = False

    async def _disconnect_client(self) -> None:
        """Disconnect and release adapter connection slots."""
        if self._client is None:
            self._connected = False
            return

        client = self._client
        self._client = None
        self._connected = False
        try:
            if client.is_connected:
                await client.disconnect()
        except BleakError as error:
            _LOGGER.debug("Ignoring disconnect error for %s: %s", self.mac, error)

    async def _trigger_active_scan_burst(self) -> None:
        """Run a short active scan to wake sleeping locks."""
        try:
            await bluetooth.async_request_active_scan(
                self._hass, ACTIVE_SCAN_BURST_SECONDS
            )
        except (AttributeError, TypeError):
            _LOGGER.debug("Active scan request unavailable")

    def _advertisement_age(
        self, service_info: bluetooth.BluetoothServiceInfoBleak
    ) -> float:
        """Return advertisement age using the same clock as Home Assistant."""
        return monotonic_time_coarse() - service_info.time

    def _is_fresh_advertisement(
        self, service_info: bluetooth.BluetoothServiceInfoBleak
    ) -> bool:
        """Return True when the advertisement is recent enough to connect."""
        return self._advertisement_age(service_info) <= CONNECTABLE_ADVERTISEMENT_MAX_AGE

    def _fresh_service_info(
        self,
    ) -> bluetooth.BluetoothServiceInfoBleak | None:
        """Return the last connectable advertisement if it is still fresh."""
        last = bluetooth.async_last_service_info(
            self._hass, self.mac, connectable=True
        )
        if last and self._is_fresh_advertisement(last):
            return last
        return None

    async def _wait_for_connectable_advertisement(
        self,
        timeout: int | None = None,
        op_id: str = "-",
    ) -> bluetooth.BluetoothServiceInfoBleak:
        """Wait until the lock sends a fresh connectable advertisement."""
        normalized_mac = self.mac.upper()
        wait_timeout = timeout or ADVERTISEMENT_WAIT_TIMEOUT
        wait_start = monotonic_time_coarse()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[bluetooth.BluetoothServiceInfoBleak] = loop.create_future()

        _LOGGER.info(
            "[%s] %s (%s): waiting up to %ss for advertisement",
            op_id,
            self.name or self.mac,
            self.mac,
            wait_timeout,
        )

        fresh = self._fresh_service_info()
        if fresh:
            _LOGGER.info(
                "[%s] %s: using cached advertisement (age=%.1fs, no wait needed)",
                op_id,
                self.mac,
                self._advertisement_age(fresh),
            )
            return fresh

        @callback
        def _async_bluetooth_callback(
            service_info: bluetooth.BluetoothServiceInfoBleak,
            _change: bluetooth.BluetoothChange,
        ) -> None:
            if done.done():
                return
            if service_info.address.upper() != normalized_mac:
                return
            age = self._advertisement_age(service_info)
            if age > CONNECTABLE_ADVERTISEMENT_MAX_AGE:
                _LOGGER.debug(
                    "[%s] Ignoring stale advertisement for %s (age=%.1fs)",
                    op_id,
                    self.mac,
                    age,
                )
                return
            done.set_result(service_info)

        remove_callback = bluetooth.async_register_callback(
            self._hass,
            _async_bluetooth_callback,
            {ADDRESS: self.mac},
            bluetooth.BluetoothScanningMode.PASSIVE,
        )
        deadline = loop.time() + wait_timeout
        try:
            await self._trigger_active_scan_burst()
            while not done.done():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    service_info = await asyncio.wait_for(done, timeout=remaining)
                except TimeoutError:
                    if loop.time() >= deadline:
                        break
                    await self._trigger_active_scan_burst()
                    continue
                _LOGGER.info(
                    "[%s] %s: advertisement received after %.1fs (rssi=%s, source=%s)",
                    op_id,
                    self.mac,
                    monotonic_time_coarse() - wait_start,
                    service_info.rssi,
                    service_info.source,
                )
                return service_info

            fresh = self._fresh_service_info()
            if fresh:
                _LOGGER.info(
                    "[%s] %s (%s): using last connectable advertisement after %.1fs (age=%.1fs)",
                    op_id,
                    self.name or self.mac,
                    self.mac,
                    monotonic_time_coarse() - wait_start,
                    self._advertisement_age(fresh),
                )
                return fresh

            diagnostics = "unavailable"
            try:
                diagnostics = bluetooth.async_address_reachability_diagnostics(
                    self._hass,
                    self.mac,
                    bluetooth.BluetoothReachabilityIntent.CONNECTION,
                )
            except (TypeError, AttributeError):
                _LOGGER.debug(
                    "Reachability diagnostics unavailable for %s", self.mac
                )
            _LOGGER.warning(
                "[%s] Lock %s (%s) did not send a fresh advertisement within %ss "
                "(waited %.1fs). Diagnostics: %s",
                op_id,
                self.name or self.mac,
                self.mac,
                wait_timeout,
                monotonic_time_coarse() - wait_start,
                diagnostics,
            )
            raise BleakError(
                f"Lock {self.mac} did not advertise within "
                f"{wait_timeout}s. Wake the lock in the Yeelock app and try again."
            )
        finally:
            remove_callback()

    def _resolve_ble_device(self):
        """Return the freshest BLEDevice for this lock."""
        ble_device = bluetooth.async_ble_device_from_address(
            self._hass, self.mac, connectable=True
        )
        if ble_device is not None:
            self._device = ble_device
            return ble_device

        if self._device is not None:
            return self._device

        return None

    def _prepare_ble_device_for_connect(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak | None = None,
    ):
        """Return the freshest connectable BLEDevice for this lock."""
        try:
            bluetooth.async_rediscover_address(self._hass, self.mac)
        except (AttributeError, TypeError):
            _LOGGER.debug("Address rediscovery unavailable for %s", self.mac)

        ble_device = bluetooth.async_ble_device_from_address(
            self._hass, self.mac, connectable=True
        )
        if ble_device is None and service_info is not None:
            ble_device = service_info.device
        if ble_device is not None:
            self._device = ble_device
        return ble_device

    async def _establish_yeelock_connection(
        self,
        ble_device,
        *,
        max_attempts: int = CONNECTION_MAX_ATTEMPTS,
        op_id: str = "-",
    ):
        """Connect to the lock with a single service-discovery retry."""
        try:
            return await establish_connection(
                BleakClientWithServiceCache,
                ble_device,
                self.name or self.mac,
                self._on_disconnect,
                max_attempts=max_attempts,
                use_services_cache=True,
            )
        except (BleakConnectionError, BleakError) as error:
            if "discover services" not in str(error).lower():
                raise
            _LOGGER.warning(
                "[%s] Lock %s disconnected during service discovery; waking and retrying once",
                op_id,
                self.name or self.mac,
            )
            await self._disconnect_client()
            await self._trigger_active_scan_burst()
            service_info = await self._wait_for_connectable_advertisement(
                timeout=SERVICE_DISCOVERY_RETRY_AD_TIMEOUT,
                op_id=op_id,
            )
            await asyncio.sleep(PRE_CONNECT_DELAY_SECONDS)
            ble_device = self._prepare_ble_device_for_connect(service_info)
            if ble_device is None:
                raise BleakError(
                    f"A device with address {self.mac} could not be found."
                ) from error
            return await establish_connection(
                BleakClientWithServiceCache,
                ble_device,
                self.name or self.mac,
                self._on_disconnect,
                max_attempts=max_attempts,
                use_services_cache=True,
            )

    async def _connect(
        self,
        *,
        wait_for_advertisement: bool = True,
        advertisement_timeout: int | None = None,
        op_id: str = "-",
    ):
        """Connect to the device.

        :raises BleakError: if the device is not found
        """
        async with self._connect_lock:
            if self._client is not None and self._client.is_connected:
                return

            if self._client is not None and not self._client.is_connected:
                await self._disconnect_client()

            self._connecting = True
            try:
                if wait_for_advertisement:
                    service_info = await self._wait_for_connectable_advertisement(
                        timeout=advertisement_timeout,
                        op_id=op_id,
                    )
                    await asyncio.sleep(PRE_CONNECT_DELAY_SECONDS)
                    ble_device = self._prepare_ble_device_for_connect(service_info)
                else:
                    ble_device = self._resolve_ble_device()

                if not ble_device:
                    raise BleakError(
                        f"A device with address {self.mac} could not be found."
                    )

                connect_start = monotonic_time_coarse()
                _LOGGER.debug("[%s] Connecting to %s", op_id, self.mac)
                try:
                    self._client = await asyncio.wait_for(
                        self._establish_yeelock_connection(ble_device, op_id=op_id),
                        timeout=CONNECT_PHASE_TIMEOUT,
                    )
                except TimeoutError as error:
                    raise BleakError(
                        f"Connect timeout: {self.mac} did not connect within "
                        f"{CONNECT_PHASE_TIMEOUT:.0f}s (adapter likely congested)"
                    ) from error
                self._connected = True
                _LOGGER.info(
                    "[%s] %s: BLE connected after %.1fs",
                    op_id,
                    self.name or self.mac,
                    monotonic_time_coarse() - connect_start,
                )
                await self._client.start_notify(
                    uuid.UUID(UUID_NOTIFY), self._handle_data
                )
                _LOGGER.debug("[%s] Listening for notifications from %s", op_id, self.mac)
            except Exception:
                await self._disconnect_client()
                raise
            finally:
                self._connecting = False

    async def _request_battery_on_connection(self) -> None:
        """Request battery level on the active BLE connection."""
        if self._client is None or not self._client.is_connected:
            return
        _LOGGER.debug("Requesting battery level for %s", self.mac)
        await self._client.write_gatt_char(
            uuid.UUID(UUID_COMMAND), bytearray(self._encrypt_battery())
        )
        await asyncio.sleep(NOTIFICATION_WAIT_SECONDS)

    async def _handle_data(self, sender, value):
        """Handle data notifications."""
        _LOGGER.debug("Received notification from %s (len=%s)", sender, len(value))
        if not value:
            _LOGGER.warning("Received empty notification from %s", sender)
            return
        new_state = None
        first_byte = value[0]

        # Lock change successes
        # Unlocking
        if first_byte == 0x2:
            new_state = "unlocking"

        # Unlocked
        elif first_byte == 0x3:
            new_state = "unlocked"

        # Locking
        elif first_byte == 0x4:
            new_state = "locking"

        # Locked
        elif first_byte == 0x5:
            new_state = "locked"

        # Lock change failures
        # Invalid signing key
        elif first_byte == 0xFF:
            _LOGGER.error("Invalid signing key for %s (%s)", self.name, self.mac)
            new_state = "jammed"

        # Time needs to be synced
        elif first_byte == 0x9:
            _LOGGER.info("Lock reported time drift; syncing time")
            await self._time_sync_on_connection()
            if self._last_action:
                _LOGGER.debug(
                    "Retrying last action after time sync: %s", self._last_action
                )
                await self._client.write_gatt_char(
                    uuid.UUID(UUID_COMMAND),
                    bytearray(self._encrypt(LOCKER_KIND[self._last_action])),
                )
                self._last_action = None

        # Battery response notification
        elif first_byte == 0x7:
            if len(value) > 6:
                self.battery_level = value[6]
                _LOGGER.debug("Received battery level: %s%%", self.battery_level)
                if self._battery_sensor is not None:
                    await self._battery_sensor._update_battery_level(self.battery_level)

                await self._maybe_auto_unlock_low_battery()
            else:
                _LOGGER.warning(
                    "Battery notification too short (len=%s) from %s",
                    len(value),
                    sender,
                )

        # Unknown notification received
        else:
            _LOGGER.warning("Unknown notification received (0x%02x)", first_byte)

        # Update to the new lock state, if we have one
        if new_state is not None:
            _LOGGER.debug("Notified of %s", new_state)
            self._resolve_command_state_waiter(new_state)
            if self._lock is not None:
                await self._lock._update_lock_state(new_state)

    def _encrypt_command(
        self, command: int, admin_identification_mode: int, payload: bytes = b""
    ) -> bytes:
        """Encrypt a command packet.

        The protocol frames are 20 bytes long and include:
        command + admin mode + timestamp + optional payload + HMAC-SHA1 fragment.
        """
        key = bytearray.fromhex(self.key)
        timestamp = int(time())

        message = (
            command.to_bytes(1, "big")
            + admin_identification_mode.to_bytes(1, "big")
            + timestamp.to_bytes(4, "big")
            + payload
        )
        signature_length = 20 - len(message)
        hmac_result = bytearray.fromhex(
            hmac.new(key, message, hashlib.sha1).hexdigest()
        )[:signature_length]
        return message + hmac_result

    def _encrypt(self, unlock_mode):
        """Encrypt lock and unlock command packets."""
        output_value = self._encrypt_command(
            command=0x01,
            admin_identification_mode=0x50,
            payload=int(unlock_mode, 16).to_bytes(1, "big"),
        )
        _LOGGER.debug("Prepared transactional command payload")
        return output_value

    def _encrypt_time(self):
        """Encrypt the time sync command packet."""
        output_value = self._encrypt_command(command=0x08, admin_identification_mode=0x40)
        _LOGGER.debug("Prepared time sync command payload")
        return output_value

    def _encrypt_battery(self):
        """Encrypt the battery request command packet."""
        output_value = self._encrypt_command(command=0x06, admin_identification_mode=0x40)
        _LOGGER.debug("Prepared battery request command payload")
        return output_value

    async def _send_lock_command(self, kind: str, op_id: str) -> None:
        """Write the lock/unlock command to the active connection."""
        loop = asyncio.get_running_loop()
        self._command_state_waiter = loop.create_future()
        _LOGGER.debug("[%s] Sending %s command to %s", op_id, kind, self.mac)
        await self._client.write_gatt_char(
            uuid.UUID(UUID_COMMAND), bytearray(self._encrypt(LOCKER_KIND[kind]))
        )

    async def locker(self, kind) -> None:
        """Lock, unlock and quick unlock the device."""
        if self._active_op_kind is not None:
            _LOGGER.info(
                "%s: ignoring %s request, %s is already in progress",
                self.name or self.mac,
                kind,
                self._active_op_kind,
            )
            raise BleakError(
                f"{self.name or self.mac} is already processing "
                f"{self._active_op_kind}; please wait for it to finish "
                "instead of pressing again."
            )

        self._last_action = kind
        self._active_op_kind = kind
        op_id = uuid.uuid4().hex[:8]
        op_start = monotonic_time_coarse()

        try:
            cooldown_remaining = self._locker_cooldown_until - op_start
            if cooldown_remaining > 0:
                _LOGGER.info(
                    "[%s] %s: waiting %.0fs for adapter to recover before retrying",
                    op_id,
                    self.name or self.mac,
                    cooldown_remaining,
                )
                await asyncio.sleep(cooldown_remaining)

            _LOGGER.info("[%s] %s: %s requested", op_id, self.name or self.mac, kind)

            async with _adapter_session(self._hass):
                try:
                    await self._connect(
                        advertisement_timeout=LOCK_ADVERTISEMENT_WAIT_TIMEOUT,
                        op_id=op_id,
                    )
                    try:
                        await self._send_lock_command(kind, op_id)
                    except BleakError as error:
                        if not any(
                            marker in str(error).lower()
                            for marker in ("unlikely", "protocol error")
                        ):
                            raise
                        _LOGGER.warning(
                            "[%s] %s: GATT protocol error, reconnecting and retrying once: %s",
                            op_id,
                            self.name or self.mac,
                            error,
                        )
                        await self._disconnect_client()
                        await self._connect(
                            advertisement_timeout=SERVICE_DISCOVERY_RETRY_AD_TIMEOUT,
                            op_id=op_id,
                        )
                        await self._send_lock_command(kind, op_id)
                    await self._wait_for_command_result(kind)
                    _LOGGER.info(
                        "[%s] %s: %s confirmed after %.1fs total",
                        op_id,
                        self.name or self.mac,
                        kind,
                        monotonic_time_coarse() - op_start,
                    )
                except BleakError as error:
                    _LOGGER.error(
                        "[%s] %s: %s failed after %.1fs: %s",
                        op_id,
                        self.name or self.mac,
                        kind,
                        monotonic_time_coarse() - op_start,
                        error,
                    )
                    error_text = str(error).lower()
                    if "connect timeout" in error_text:
                        # We gave up waiting, but BlueZ/the controller may
                        # still be working on the abandoned attempt in the
                        # background. Proactively ask BlueZ to drop it and
                        # cool down longer before grabbing that slot again.
                        try:
                            await close_stale_connections_by_address(self.mac)
                        except Exception as cleanup_error:  # noqa: BLE001
                            _LOGGER.debug(
                                "[%s] %s: stale connection cleanup failed: %s",
                                op_id,
                                self.name or self.mac,
                                cleanup_error,
                            )
                        self._locker_cooldown_until = (
                            monotonic_time_coarse() + CONNECT_TIMEOUT_COOLDOWN_SECONDS
                        )
                    elif any(
                        marker in error_text
                        for marker in ("connection slot", "discover services", "timeout")
                    ):
                        self._locker_cooldown_until = (
                            monotonic_time_coarse() + LOCKER_FAILURE_COOLDOWN_SECONDS
                        )
                    raise
                finally:
                    self._command_state_waiter = None
                    await self._disconnect_client()
        finally:
            self._active_op_kind = None

    async def _time_sync_on_connection(self) -> None:
        """Sync time on the active BLE connection."""
        if self._client is None or not self._client.is_connected:
            return
        _LOGGER.debug("Time sync start for %s", self.mac)
        await self._client.write_gatt_char(
            uuid.UUID(UUID_COMMAND), bytearray(self._encrypt_time())
        )
        await asyncio.sleep(NOTIFICATION_WAIT_SECONDS)

    async def time_sync(self) -> None:
        """Time sync and retry."""
        if self._client is not None and self._client.is_connected:
            await self._time_sync_on_connection()
            return

        async with _adapter_session(self._hass):
            try:
                await self._connect(wait_for_advertisement=False)
                await self._time_sync_on_connection()
            except BleakError as error:
                _LOGGER.error("BleakError: %s", error)
                raise
            finally:
                await self._disconnect_client()

    async def update_battery(self) -> None:
        """Request battery level from the lock over BLE."""
        async with _adapter_session(self._hass):
            try:
                await self._connect()
                await self._request_battery_on_connection()
            except BleakError as error:
                _LOGGER.error("BleakError: %s", error)
                raise
            except Exception as error:  # pragma: no cover - backend-specific transient failures
                _LOGGER.warning("Unable to update battery for %s: %s", self.mac, error)
                raise
            finally:
                await self._disconnect_client()

    async def _maybe_auto_unlock_low_battery(self) -> None:
        """Unlock the lock automatically when battery is critically low."""
        if self.battery_level is None:
            return

        if not self.auto_unlock_low_battery:
            self._auto_unlock_triggered = False
            return

        if self.battery_level > self.auto_unlock_low_battery_threshold:
            self._auto_unlock_triggered = False
            return

        if self._auto_unlock_triggered:
            return

        if self._lock is not None and not self._lock.is_locked:
            self._auto_unlock_triggered = True
            return

        _LOGGER.warning(
            "Battery level (%s%%) is at or below %s%%, attempting automatic unlock",
            self.battery_level,
            self.auto_unlock_low_battery_threshold,
        )
        self._auto_unlock_triggered = True
        self._hass.async_create_task(self.locker("unlock"))
