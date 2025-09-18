# statext_dependencies_load

## Location
[src/backend/statistics/dependencies.c:619-652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L619-L652)

## Overview
This function loads functional dependency statistics from the PostgreSQL system catalog for a specified statistics object, enabling the optimizer to consider column dependencies during query planning.

## Definition


## Detailed Description
The function retrieves functional dependency statistics from the  system catalog. It performs a cache lookup using the statistics object OID and inheritance flag, then deserializes the stored dependency data into an in-memory  structure. This enables PostgreSQL's query optimizer to account for statistical dependencies between columns when estimating selectivity and cardinality.

The function follows PostgreSQL's standard pattern for loading statistics data: cache lookup, null checking, data extraction, deserialization, and cache cleanup. If the requested statistics kind is not available or the cache lookup fails, appropriate error messages are generated.

## Parameters / Member Variables
- : Object ID of the multivariate statistics object whose dependencies should be loaded
- : Boolean flag indicating whether to load statistics for inherited tables (true) or only the base table (false)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) (system cache lookup)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (attribute extraction from cached tuple)
  - [statext_dependencies_deserialize](statext_dependencies_deserialize.md) (converts binary data to MVDependencies structure)
  - DatumGetByteaPP (extracts bytea data from Datum)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cleanup cached tuple)
  - STATS_EXT_DEPENDENCIES (constant identifying dependency statistics type)

- Called from (representative examples):
  - [dependencies_clauselist_selectivity](../d/dependencies_clauselist_selectivity.md) (for selectivity estimation using dependencies)

## Notes and Other Information
- Throws ERROR if the cache lookup fails or if dependency statistics haven't been built yet
- The returned MVDependencies structure must be freed by the caller
- Part of PostgreSQL's extended statistics framework introduced to handle multi-column dependencies
- Works with the ANALYZE command's dependency collection mechanism
- The  parameter supports table inheritance hierarchies in PostgreSQL