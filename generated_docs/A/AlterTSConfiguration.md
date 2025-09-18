# AlterTSConfiguration

## Location
src/backend/commands/tsearchcmds.c: 1156 - 1203

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