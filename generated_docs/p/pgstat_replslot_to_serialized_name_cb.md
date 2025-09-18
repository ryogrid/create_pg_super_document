# pgstat_replslot_to_serialized_name_cb

## Location
src/backend/utils/activity/pgstat_replslot.c: 189 - 201

## Overview
A callback function that converts a replication slot statistics key to its serialized name representation during PostgreSQL shutdown.

## Definition
```c
void pgstat_replslot_to_serialized_name_cb(const PgStat_HashKey *key, const PgStatShared_Common *header, NameData *name)
```

## Detailed Description
This function is part of PostgreSQL's statistics subsystem and is specifically responsible for converting replication slot statistics keys to their corresponding slot names during the serialization process. It's designed to be called only during late shutdown when the set of existing replication slots is stable and not allowed to change. The function uses the object ID from the statistics key to look up the actual replication slot name and store it in the provided NameData structure.

The function includes safety checks and will raise an ERROR if it cannot find a name for the given replication slot index, which should not happen under normal circumstances during shutdown.

## Parameters / Member Variables
- `key`: Pointer to a PgStat_HashKey structure containing the object ID (replication slot index) to be converted
- `header`: Pointer to PgStatShared_Common header (currently unused in this implementation)
- `name`: Pointer to NameData structure where the resolved slot name will be stored

## Dependencies
- Functions called/Symbols referenced:
  - ReplicationSlotName
  - elog
- Types referenced:
  - PgStat_HashKey
  - PgStatShared_Common
  - NameData
- Called from (representative examples):
  - Statistics hash table operations (via SH_DECLARE macro in pgstat.c:317)

## Notes and Other Information
- This function is designed to be called only during late shutdown phases
- The replication slot set must be stable when this function is called
- Part of the broader PostgreSQL statistics serialization framework
- Located in src/backend/utils/activity/pgstat_replslot.c:189-201