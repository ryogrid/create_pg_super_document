# RelationMapUpdateMap

## Location
src/backend/utils/cache/relmapper.c: 325 - 382

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
  - RelFileNumber (parameter type)
  - RelMapFile (structure type for mapping tables)
  - IsBootstrapProcessingMode (checks if in bootstrap mode)
  - GetCurrentTransactionNestLevel (ensures not in subtransaction)
  - IsInParallelMode (ensures not in parallel execution)
  - apply_map_update (applies the actual mapping change)
- Called from (representative examples):
  - swap_relation_files (cluster.c:1179, 1180)
  - formrdesc (relcache.c:1997)
  - RelationBuildLocalRelation (relcache.c:3704)
  - RelationSetNewRelfilenumber (relcache.c:3917)

## Notes and Other Information
- Does not support mapping changes within subtransactions due to complexity of required bookkeeping
- Prohibited in parallel mode to avoid concurrency complications
- Bootstrap mode bypasses normal transaction handling and updates permanent maps directly
- The immediate flag allows for context-sensitive activation timing
- Uses apply_map_update as the low-level implementation for actually modifying the mapping structures
- Critical for operations like relation file swapping during CLUSTER and relation recreation scenarios