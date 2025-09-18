# RemoveTSConfigurationById

## Location
src/backend/commands/tsearchcmds.c: 1108 - 1155

## Overview
Removes a text search configuration and all its associated token-dictionary mappings from the system catalogs by OID.

## Definition
```c
void RemoveTSConfigurationById(Oid cfgId)
```

## Detailed Description
RemoveTSConfigurationById performs the low-level deletion of a text search configuration from the system catalogs. It first removes the main configuration entry from pg_ts_config, then systematically deletes all associated token-dictionary mapping entries from pg_ts_config_map. The function is called by the dependency system during DROP operations and assumes that dependency checks have already been performed by the caller.

## Parameters / Member Variables
- `cfgId`: The OID of the text search configuration to remove from the system catalogs

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens system catalog relations)
  - [SearchSysCache1](../S/SearchSysCache1.md) (finds configuration tuple by OID)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md) (removes tuples from catalogs)
  - [ReleaseSysCache](ReleaseSysCache.md) (releases cached tuple)
  - [ScanKeyInit](../S/ScanKeyInit.md) (initializes scan key)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext (scans configuration map)
  - [systable_endscan](../s/systable_endscan.md) (ends scan)
  - table_close (closes relations)
- Called from (representative examples):
  - [doDeletion](../d/doDeletion.md) (dependency system deletion)

## Notes and Other Information
- Uses RowExclusiveLock on both pg_ts_config and pg_ts_config_map relations
- Includes error handling for missing configurations with detailed error message
- Note: Error message incorrectly refers to "text search dictionary" instead of "configuration"
- Does not handle dependency checking - assumes caller has verified safe deletion
- Scans pg_ts_config_map using TSConfigMapIndexId for efficient map entry removal
- Part of the PostgreSQL dependency system's deletion framework
- Called during CASCADE deletions and explicit DROP CONFIGURATION commands