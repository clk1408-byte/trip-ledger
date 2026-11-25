# Trip Ledger

Trip itineraries and shared trip costs for two, installable on a phone and usable offline.

- The app lives here and is served by GitHub Pages. It holds no personal data.
- Trips, bookings, expenses and attachments live in a separate **private** repository (`trip-ledger-data`). Each phone keeps a full copy and syncs with it when online.
- The vault is encrypted on the phone (AES-256-GCM, key from a passphrase via PBKDF2 or from a fingerprint passkey via WebAuthn PRF) before it syncs.

## Data layout (private repo)
- `trips/<id>.json`: one trip
- `items/<id>.json`: one booking, plan item, expense or payment
- `attachments/<id>`: raw file bytes (vault photos are encrypted)
- `vault.json`: encrypted vault
