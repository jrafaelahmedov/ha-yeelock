"""Yeelock device."""

import asyncio
import hashlib
import hmac
import logging
import time as time_module
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import time

from bleak.exc import BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.match import ADDRESS
from homeassistant.const import CONF_API_KEY, CONF_MAC, CONF_MODEL, CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr

from .const import (
    ADVERTISEMENT_WAIT_TIMEOUT,
    BLE_SEMAPHORE_KEY,
    CONF_AUTO_UNLOCK_LOW_BATTERY,
    CONF_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
    CONNECTION_MAX_ATTEMPTS,
    DEFAULT_AUTO_UNLOCK_LOW_BATTERY,
    DEFAULT_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD,
    DOMAIN,
    FRESH_ADVERTISEMENT_MAX_AGE,
    LOCKER_KIND,
    NOTIFICATION_WAIT_SECONDS,
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

    async def _wait_for_connectable_advertisement(
        self,
    ) -> bluetooth.BluetoothServiceInfoBleak:
        """Wait until the lock sends a fresh connectable advertisement."""
        normalized_mac = self.mac.upper()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[bluetooth.BluetoothServiceInfoBleak] = loop.create_future()

        _LOGGER.info(
            "Waiting up to %ss for %s (%s) to advertise",
            ADVERTISEMENT_WAIT_TIMEOUT,
            self.name or self.mac,
            self.mac,
        )

        @callback
        def _async_bluetooth_callback(
            service_info: bluetooth.BluetoothServiceInfoBleak,
            _change: bluetooth.BluetoothChange,
        ) -> None:
            if done.done():
                return
            if service_info.address.upper() != normalized_mac:
                return
            age = time_module.monotonic() - service_info.time
            if age > FRESH_ADVERTISEMENT_MAX_AGE:
                _LOGGER.debug(
                    "Ignoring stale advertisement for %s (age=%.1fs)",
                    self.mac,
                    age,
                )
                return
            _LOGGER.debug(
                "Fresh advertisement for %s (age=%.1fs, rssi=%s, source=%s)",
                self.mac,
                age,
                service_info.rssi,
                service_info.source,
            )
            done.set_result(service_info)

        remove_callback = bluetooth.async_register_callback(
            self._hass,
            _async_bluetooth_callback,
            {ADDRESS: self.mac},
            bluetooth.BluetoothScanningMode.ACTIVE,
        )
        try:
            return await asyncio.wait_for(done, timeout=ADVERTISEMENT_WAIT_TIMEOUT)
        except TimeoutError as error:
            diagnostics = bluetooth.async_address_reachability_diagnostics(
                self._hass, self.mac
            )
            _LOGGER.warning(
                "Lock %s (%s) did not send a fresh advertisement within %ss. "
                "Diagnostics: %s",
                self.name or self.mac,
                self.mac,
                ADVERTISEMENT_WAIT_TIMEOUT,
                diagnostics,
            )
            raise BleakError(
                f"Lock {self.mac} did not advertise within "
                f"{ADVERTISEMENT_WAIT_TIMEOUT}s. Wake the lock in the "
                "Yeelock app and try again."
            ) from error
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

    async def _connect(self, *, wait_for_advertisement: bool = True):
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
                    service_info = await self._wait_for_connectable_advertisement()
                    ble_device = service_info.device
                    self._device = ble_device
                else:
                    ble_device = self._resolve_ble_device()

                if not ble_device:
                    raise BleakError(
                        f"A device with address {self.mac} could not be found."
                    )

                _LOGGER.debug("Connecting to %s", self.mac)
                self._client = await establish_connection(
                    BleakClientWithServiceCache,
                    ble_device,
                    self.name or self.mac,
                    self._on_disconnect,
                    max_attempts=CONNECTION_MAX_ATTEMPTS,
                )
                self._connected = True
                _LOGGER.debug("Connected to %s", self.mac)
                await self._client.start_notify(
                    uuid.UUID(UUID_NOTIFY), self._handle_data
                )
                _LOGGER.debug("Listening for notifications from %s", self.mac)
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

    async def locker(self, kind) -> None:
        """Lock, unlock and quick unlock the device."""
        self._last_action = kind
        async with _adapter_session(self._hass):
            try:
                await self._connect()
                _LOGGER.debug("Sending %s command to %s", kind, self.mac)
                await self._client.write_gatt_char(
                    uuid.UUID(UUID_COMMAND), bytearray(self._encrypt(LOCKER_KIND[kind]))
                )
                await asyncio.sleep(NOTIFICATION_WAIT_SECONDS)
                if self._battery_sensor is not None:
                    await self._request_battery_on_connection()
            except BleakError as error:
                _LOGGER.error("BleakError for %s (%s): %s", self.name, self.mac, error)
                raise
            finally:
                await self._disconnect_client()

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
