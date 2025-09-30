# RelationMapUpdateMap

## Location
[src/backend/utils/cache/relmapper.c:325-382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L325-L382)

## Overview
Installs a new relation-to-file mapping for a specified relation, with support for immediate activation or pending updates that take effect at command completion.

## Definition
void RelationMapUpdateMap(Oid relationId, RelFileNumber fileNumber, bool shared, bool immediate)

## Detailed Description
This function is the primary interface for updating relation mappings in PostgreSQL. It handles the installation of new relation-to-file number mappings with different activation strategies depending on the execution context and requirements.

The function supports multiple operational modes:
- Bootstrap mode: Updates are applied directly to the permanent mapping tables
- Normal mode with immediate flag: Updates are applied to active update maps for immediate visibility
- Normal mode without immediate flag: Updates are queued as pending changes that activate at CommandCounterIncrement

The function includes important safety checks to prevent mapping changes in unsupported contexts like subtransactions or parallel mode, as these would require additional bookkeeping infrastructure that is currently not implemented.

## Parameters / Member Variables
- `relationId`: OID of the relation whose mapping is being updated
- `fileNumber`: New RelFileNumber to associate with the relation
- `shared`: Boolean indicating whether this is a shared relation (true) or local relation (false)
- `immediate`: Boolean controlling whether the mapping takes effect immediately (true) or is deferred until CommandCounterIncrement (false)

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileNumber](RelFileNumber.md) (parameter type)
  - [RelMapFile](RelMapFile.md) (structure type for mapping tables)
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md) (ensures not in subtransaction)
  - [IsInParallelMode](../I/IsInParallelMode.md) (ensures not in parallel execution)
  - [apply_map_update](../a/apply_map_update.md) (applies the actual mapping change)
- Called from (representative examples):
  - [swap_relation_files](../s/swap_relation_files.md) (cluster.c:1179, 1180)
  - [formrdesc](../f/formrdesc.md) (relcache.c:1997)
  - [RelationBuildLocalRelation](RelationBuildLocalRelation.md) (relcache.c:3704)
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md) (relcache.c:3917)

## Notes and Other Information
- Does not support mapping changes within subtransactions due to complexity of required bookkeeping
- Prohibited in parallel mode to avoid concurrency complications
- Bootstrap mode bypasses normal transaction handling and updates permanent maps directly
- The immediate flag allows for context-sensitive activation timing
- Uses apply_map_update as the low-level implementation for actually modifying the mapping structures
- Critical for operations like relation file swapping during CLUSTER and relation recreation scenarios

## Simplified Source

```c
void RelationMapUpdateMap(Oid relationId, RelFileNumber fileNumber,
                         bool shared, bool immediate) {
    RelMapFile *map;

    if (IsBootstrapProcessingMode()) {
        // Bootstrap mode: update permanent map directly
        if (shared)
            map = &shared_map;
        else
            map = &local_map;
    }
    else {
        // Safety checks: no subtransactions or parallel mode
        if (GetCurrentTransactionNestLevel() > 1)
            elog(ERROR, "cannot change relation mapping within subtransaction");

        if (IsInParallelMode())
            elog(ERROR, "cannot change relation mapping in parallel mode");

        if (immediate) {
            // Make active immediately
            if (shared)
                map = &active_shared_updates;
            else
                map = &active_local_updates;
        }
        else {
            // Make pending until CommandCounterIncrement
            if (shared)
                map = &pending_shared_updates;
            else
                map = &pending_local_updates;
        }
    }

    // Apply the mapping change
    apply_map_update(map, relationId, fileNumber, true);
}
```