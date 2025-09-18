# pgstat_replslot_from_serialized_name_cb

## Location
src/backend/utils/activity/pgstat_replslot.c: 202 - 217

## Overview
A callback function that converts a serialized replication slot name back to its corresponding statistics hash key during PostgreSQL startup or statistics deserialization.

## Definition
```c
bool pgstat_replslot_from_serialized_name_cb(const NameData *name, PgStat_HashKey *key)
```

## Detailed Description
This function serves as the reverse operation of pgstat_replslot_to_serialized_name_cb, converting a serialized replication slot name back into a PgStat_HashKey structure. It's part of PostgreSQL's statistics subsystem and is used during startup or when deserializing statistics data. The function looks up the replication slot index using the provided name and constructs the appropriate hash key. If the slot no longer exists (perhaps it was deleted), the function returns false to indicate the conversion failed.

The function handles the case where a replication slot might have been deleted between shutdown and startup, making it robust against configuration changes.

## Parameters / Member Variables
- `name`: Pointer to a NameData structure containing the serialized replication slot name
- `key`: Pointer to PgStat_HashKey structure that will be populated with the converted key information

## Dependencies
- Functions called/Symbols referenced:
  - [get_replslot_index](../g/get_replslot_index.md)
  - NameStr (macro)
- Types/Constants referenced:
  - [NameData](../N/NameData.md)
  - PgStat_HashKey
  - PGSTAT_KIND_REPLSLOT
  - InvalidOid
- Called from (representative examples):
  - Statistics hash table operations (via SH_DECLARE macro in pgstat.c:318)

## Notes and Other Information
- Returns true on successful conversion, false if the slot no longer exists
- Handles the case where replication slots might have been deleted between shutdown and startup
- Sets the key kind to PGSTAT_KIND_REPLSLOT and dboid to InvalidOid
- The objoid field is set to the slot index returned by get_replslot_index
- Part of the broader PostgreSQL statistics deserialization framework
- Located in src/backend/utils/activity/pgstat_replslot.c:202-217