# AlterTSConfiguration

## Location
[src/backend/commands/tsearchcmds.c:1156-1203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L1156-L1203)

## Overview
Main entry point for ALTER TEXT SEARCH CONFIGURATION commands, handling token-dictionary mapping changes and dependency updates.

## Definition
```c
ObjectAddress AlterTSConfiguration(AlterTSConfigurationStmt *stmt)
```

## Detailed Description
AlterTSConfiguration implements the ALTER TEXT SEARCH CONFIGURATION SQL command by processing mapping additions or deletions based on the statement type. It validates the configuration exists and checks ownership permissions, then delegates to specialized functions for adding (MakeConfigurationMapping) or removing (DropConfigurationMapping) token-dictionary mappings. After modifying mappings, it updates all dependency relationships and triggers post-alter hooks.

## Parameters / Member Variables
- `stmt`: AlterTSConfigurationStmt containing the configuration name and operation details (dicts for ADD MAPPING, tokentype for DROP MAPPING)

## Dependencies
- Functions called/Symbols referenced:
  - [GetTSConfigTuple](../G/GetTSConfigTuple.md) (finds configuration by name)
  - [object_ownercheck](../o/object_ownercheck.md) (verifies ownership permission)
  - [NameListToString](../N/NameListToString.md) (formats names for error messages)
  - [MakeConfigurationMapping](../M/MakeConfigurationMapping.md) (adds token-dictionary mappings)
  - [DropConfigurationMapping](../D/DropConfigurationMapping.md) (removes token-dictionary mappings)
  - [makeConfigurationDependencies](../m/makeConfigurationDependencies.md) (updates all dependencies)
  - InvokeObjectPostAlterHook (triggers post-alter hooks)
  - ObjectAddressSet (constructs return address)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (SQL command processing)

## Notes and Other Information
- Requires ownership of the configuration being altered
- Uses RowExclusiveLock on pg_ts_config_map relation
- Supports two operation types: ADD MAPPING (stmt->dicts) and DROP MAPPING (stmt->tokentype)
- Updates dependencies after each alteration to maintain consistency
- Returns ObjectAddress for use in dependency tracking and event triggers
- Part of the PostgreSQL text search infrastructure
- Generates detailed error messages using configuration names for better user experience

## Simplified Source

```c
ObjectAddress AlterTSConfiguration(AlterTSConfigurationStmt *stmt) {
    HeapTuple tup;
    Oid cfgId;
    Relation relMap;
    ObjectAddress address;

    // Find the text search configuration
    tup = GetTSConfigTuple(stmt->cfgname);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("text search configuration \"%s\" does not exist",
                        NameListToString(stmt->cfgname))));
    }

    cfgId = ((Form_pg_ts_config) GETSTRUCT(tup))->oid;

    // Check ownership permission
    if (!object_ownercheck(TSConfigRelationId, cfgId, GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TSCONFIGURATION,
                       NameListToString(stmt->cfgname));
    }

    // Open configuration mapping relation
    relMap = table_open(TSConfigMapRelationId, RowExclusiveLock);

    // Add or drop token-dictionary mappings
    if (stmt->dicts) {
        // ADD MAPPING operation
        MakeConfigurationMapping(stmt, tup, relMap);
    } else if (stmt->tokentype) {
        // DROP MAPPING operation
        DropConfigurationMapping(stmt, tup, relMap);
    }

    // Update all dependency relationships
    makeConfigurationDependencies(tup, true, relMap);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(TSConfigRelationId, cfgId, 0);

    // Cleanup and return
    ObjectAddressSet(address, TSConfigRelationId, cfgId);
    table_close(relMap, RowExclusiveLock);
    ReleaseSysCache(tup);

    return address;
}
```