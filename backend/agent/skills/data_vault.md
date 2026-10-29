# Pattern: Data Vault 2.0

- **Hubs** (`hub_<entity>`) — one row per business key. Columns: `<entity>_hk`
  (hash key, PK), the business key, `load_date TIMESTAMP`, `record_source STRING`.
- **Links** (`lnk_<relationship>`) — relationships between hubs. Columns: `<link>_hk`
  (PK), the hub hash keys (FKs), `load_date`, `record_source`.
- **Satellites** (`sat_<entity>_<descriptor>`) — descriptive/temporal attributes.
  Columns: parent hash key (FK to hub/link), `load_date`, `load_end_date`,
  `record_source`, `hash_diff`, plus descriptive columns.
- Use SHA-256 (or MD5) for hash keys, computed from concatenated, standardized
  business keys. Hubs/links are insert-only; satellites track change via `hash_diff`.
