"""Constants for the Yeelock integration."""

from homeassistant.const import Platform

DOMAIN = "yeelock"
BLE_SEMAPHORE_KEY = "_ble_semaphore"

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

COMMAND_FINAL_STATE = {
    "lock": "locked",
    "unlock": "unlocked",
    "unlock_quick": "unlocked",
}

COMMAND_TRANSITIONAL_STATE = {
    "lock": "locking",
    "unlock": "unlocking",
    "unlock_quick": "unlocking",
}

# Wait for a live BLE advertisement before connecting (seconds).
ADVERTISEMENT_WAIT_TIMEOUT = 30
# User lock/unlock actions wait longer because Yeelocks advertise infrequently.
LOCK_ADVERTISEMENT_WAIT_TIMEOUT = 45
# HA connectable history needs a recent advertisement (seconds).
# HA's own habluetooth stack considers connectable ads valid for ~195s;
# our stricter cutoff avoids using ancient data while still accepting
# a "just missed" advertisement instead of forcing a full new wait.
CONNECTABLE_ADVERTISEMENT_MAX_AGE = 90
# Pause after stopping active scans before opening a connection.
PRE_CONNECT_DELAY_SECONDS = 0.5
# Active scan burst to wake sleeping locks (seconds).
ACTIVE_SCAN_BURST_SECONDS = 5
# Retry BLE connections after an advertisement is observed.
CONNECTION_MAX_ATTEMPTS = 3
# Short re-wait after the lock drops during service discovery.
SERVICE_DISCOVERY_RETRY_AD_TIMEOUT = 20
# Let the Pi adapter fully release a BlueZ connection slot before retrying.
LOCKER_FAILURE_COOLDOWN_SECONDS = 20
# Bound how long we let bleak_retry_connector churn on a single connect
# attempt; its internal out-of-slots backoff can otherwise run for over
# a minute by itself, well past our own retry/backoff budget.
CONNECT_PHASE_TIMEOUT = 30
# Wait for the lock to report locked/unlocked after sending a command.
LOCK_COMMAND_RESULT_TIMEOUT = 8.0
# Keep the connection open briefly to receive lock state notifications.
NOTIFICATION_WAIT_SECONDS = 1.0
