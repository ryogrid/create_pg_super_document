# makeConfigurationDependencies

## Location
src/backend/commands/tsearchcmds.c: 812 - 898

## Overview
Creates and records all dependency relationships for a text search configuration, including dependencies on namespace, owner, parser, extension, and associated dictionaries from the configuration map.

## Definition
```c
static ObjectAddress makeConfigurationDependencies(HeapTuple tuple, bool removeOld, Relation mapRel)
```

## Detailed Description
makeConfigurationDependencies establishes the complete dependency graph for a text search configuration object. It handles both creation and update scenarios by optionally removing old dependencies first. The function creates dependencies on the configuration's namespace, owner, parser, and extension, then scans the configuration map to establish dependencies on all referenced dictionaries. It uses an ObjectAddresses list to collect and deduplicate dependencies before recording them in a single operation for efficiency.

## Parameters / Member Variables
- `tuple`: HeapTuple containing the text search configuration data from pg_ts_config
- `removeOld`: Boolean flag indicating whether to delete existing dependencies (used for ALTER operations)
- `mapRel`: Open relation handle for pg_ts_config_map, or NULL if no map scanning is needed

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_ts_config (tuple structure access)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md) (removes old dependencies)
  - [deleteSharedDependencyRecordsFor](../d/deleteSharedDependencyRecordsFor.md) (removes old shared dependencies)
  - [new_object_addresses](../n/new_object_addresses.md) (creates dependency collection)
  - [add_exact_object_address](../a/add_exact_object_address.md) (adds dependency to collection)
  - [recordDependencyOnOwner](../r/recordDependencyOnOwner.md) (records ownership dependency)
  - [recordDependencyOnCurrentExtension](../r/recordDependencyOnCurrentExtension.md) (records extension membership)
  - CommandCounterIncrement (ensures visibility of changes)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext (scans configuration map)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md) (records all dependencies)
  - [free_object_addresses](../f/free_object_addresses.md) (cleans up dependency collection)
- Called from (representative examples):
  - [DefineTSConfiguration](../D/DefineTSConfiguration.md)
  - [AlterTSConfiguration](../A/AlterTSConfiguration.md)

## Notes and Other Information
- Static function, only accessible within tsearchcmds.c
- Uses DEPENDENCY_NORMAL for all recorded dependencies
- Includes duplicate elimination through ObjectAddresses mechanism
- Performs CommandCounterIncrement to ensure visibility of caller's changes when scanning map
- Extension dependencies are preserved during removeOld operations
- Returns ObjectAddress of the configuration for caller convenience
- Scans pg_ts_config_map using TSConfigMapIndexId for efficiency