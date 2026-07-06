"""Constants for the Yeelock integration."""

from homeassistant.const import Platform

DOMAIN = "yeelock"

PLATFORMS: list[str] = [
    Platform.LOCK,
    Platform.SENSOR,
]

CONF_PHONE = "phone"
CONF_AUTO_UNLOCK_LOW_BATTERY = "auto_unlock_low_battery"
CONF_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD = "auto_unlock_low_battery_threshold"

DEFAULT_AUTO_UNLOCK_LOW_BATTERY = True
DEFAULT_AUTO_UNLOCK_LOW_BATTERY_THRESHOLD = 10

UUID_BATTERY_LEVEL = "00002a19-0000-1000-8000-00805f9b34fb"
UUID_COMMAND = "58af3dca-6fc0-4fa3-9464-74662f043a3b"
UUID_NOTIFY = "58af3dca-6fc0-4fa3-9464-74662f043a3a"

LOCKER_KIND = {
    "lock": "02",
    "unlock": "01",
    "unlock_quick": "00",
}

# Wait for a live BLE advertisement before connecting (seconds).
ADVERTISEMENT_WAIT_TIMEOUT = 30
# Only connect after an advertisement at most this many seconds old.
FRESH_ADVERTISEMENT_MAX_AGE = 5
# Retry BLE connections; newer HA Bluetooth stacks need more attempts.
CONNECTION_MAX_ATTEMPTS = 10
# Keep the connection open briefly to receive lock state notifications.
NOTIFICATION_WAIT_SECONDS = 1.5
