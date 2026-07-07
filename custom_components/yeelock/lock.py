"""Yeelock Lock."""

import logging

from bleak.exc import BleakError

from homeassistant.components.lock import LockEntity, LockEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity

from .const import COMMAND_TRANSITIONAL_STATE, DOMAIN
from .device import Yeelock, YeelockDeviceEntity


_LOGGER = logging.getLogger(__name__)

_TRANSIENT_STATES = frozenset({"locking", "unlocking"})


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
):
    """Set up the Yeelock lock platform."""
    device: Yeelock = hass.data[DOMAIN][entry.unique_id]
    lock = YeelockLock(device, hass)
    device._lock = lock  # Pass the reference
    async_add_entities([lock])
    return True


class YeelockLock(YeelockDeviceEntity, LockEntity, RestoreEntity):
    """This button locks the device."""

    _attr_name = "Lock"
    _attr_supported_features = LockEntityFeature.OPEN

    async def async_added_to_hass(self):
        """Call when entity is added to hass."""
        await super().async_added_to_hass()
        state = await self.async_get_last_state()
        if state and state.state not in _TRANSIENT_STATES:
            self._attr_state = state.state
        elif state and state.state in _TRANSIENT_STATES:
            _LOGGER.debug(
                "Discarding stale transient state %s for %s",
                state.state,
                self.device.mac,
            )
            self._attr_state = "unknown"

    @property
    def is_locking(self):
        """Return true if lock is locking."""
        return self._attr_state == "locking"

    @property
    def is_unlocking(self):
        """Return true if lock is unlocking."""
        return self._attr_state == "unlocking"

    @property
    def is_jammed(self):
        """Return true if lock is jammed."""
        return self._attr_state == "jammed"

    @property
    def is_locked(self):
        """Return true if lock is locked."""
        return self._attr_state == "locked"

    async def _update_lock_state(self, new_state):
        """Update the lock state."""
        _LOGGER.debug("Setting state to %s", new_state)
        self._attr_state = new_state
        self.async_write_ha_state()

    async def _run_lock_command(self, kind: str) -> None:
        """Run a lock command and recover from stale transitional states."""
        previous_state = self._attr_state
        already_busy = self.device._active_op_kind is not None
        if not already_busy:
            self._attr_state = COMMAND_TRANSITIONAL_STATE[kind]
            self.async_write_ha_state()
        try:
            await self.device.locker(kind)
        except (BleakError, TimeoutError) as error:
            if not already_busy and self._attr_state in _TRANSIENT_STATES:
                self._attr_state = (
                    previous_state
                    if previous_state not in _TRANSIENT_STATES
                    else "unknown"
                )
                self.async_write_ha_state()
            raise error

    async def async_lock(self):
        """Asynchronously lock."""
        await self._run_lock_command("lock")

    async def async_unlock(self):
        """Asynchronously unlock."""
        await self._run_lock_command("unlock")

    async def async_open(self):
        """Open the door quickly."""
        await self._run_lock_command("unlock_quick")
