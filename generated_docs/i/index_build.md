# index_build

## Location
src/backend/catalog/index.c: 2940 - 3132

## Overview
index_build orchestrates the complete process of building an index by invoking access-method-specific build procedures and updating catalog metadata.

## Definition


## Detailed Description
index_build is a comprehensive function that manages the entire index building process from start to finish. It begins with the index's catalog entries being valid and its physical disk file created but empty, then calls the access method's build procedure to populate the index contents. The function handles parallelization by determining worker process requirements for supported access methods like B-tree and BRIN. It manages security context by switching to the table owner's user ID to ensure index functions run with proper permissions. The function includes progress reporting, handles special cases for unlogged indexes by creating init forks, manages HOT chain consistency issues, and updates pg_class statistics for both heap and index relations. Finally, it performs exclusion constraint validation if needed and properly restores the security context.

## Parameters / Member Variables
- : The heap relation for which the index is being built
- : The index relation being constructed
- : IndexInfo structure containing index metadata and build parameters
- : Boolean indicating whether this is a reindex operation rather than initial creation
- : Boolean indicating whether parallel building should be attempted if supported

## Dependencies
- Functions called/Symbols referenced:
  - IndexInfo (structure type)
  - [IndexBuildResult](../I/IndexBuildResult.md) (structure type)
  - RelationIsValid (function)
  - PointerIsValid (function)
  - IsNormalProcessingMode (function)
  - [plan_create_index_workers](../p/plan_create_index_workers.md) (function)
  - [GetUserIdAndSecContext](../G/GetUserIdAndSecContext.md) (function)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md) (function)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md) (function)
  - [RestrictSearchPath](../R/RestrictSearchPath.md) (function)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md) (function)
  - [smgrexists](../s/smgrexists.md) (function)
  - [smgrcreate](../s/smgrcreate.md) (function)
  - [log_smgrcreate](../l/log_smgrcreate.md) (function)
  - SearchSysCacheCopy1 (function)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (function)
  - [heap_freetuple](../h/heap_freetuple.md) (function)
  - [index_update_stats](index_update_stats.md) (function)
  - CommandCounterIncrement (function)
  - [IndexCheckExclusion](../I/IndexCheckExclusion.md) (function)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md) (function)
- Called from (representative examples):
  - [build_indices](../b/build_indices.md)
  - index_create
  - index_concurrently_build
  - [reindex_index](../r/reindex_index.md)

## Notes and Other Information
- Supports parallel index building for B-tree and BRIN access methods with automatic worker planning
- Implements security restrictions by running index functions as table owner and restricting search path
- Progress reporting provides detailed status updates for monitoring long-running index builds
- Handles unlogged indexes specially by creating init forks and logging the creation for crash recovery
- Manages HOT chain consistency by setting indcheckxmin when broken HOT chains are detected during non-concurrent, non-reindex operations
- Updates statistics for both heap and index relations using index_update_stats function
- Performs exclusion constraint validation as a final step for indexes with exclusion operators
- Maintains proper transaction boundaries and security context restoration for robustness
- The function assumes relations are already opened by caller and does not close them (changed behavior from pre-8.2 PostgreSQL versions)