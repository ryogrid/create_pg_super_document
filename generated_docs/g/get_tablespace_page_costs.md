# get_tablespace_page_costs

## Location
[src/backend/utils/cache/spccache.c:182-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/spccache.c#L182-L214)

## Overview
Retrieves the random and sequential page cost parameters for a specified tablespace, falling back to global defaults when tablespace-specific values are not configured.

## Definition


## Detailed Description
This function provides the query planner with tablespace-specific I/O cost parameters needed for accurate cost estimation. It fetches the cached tablespace entry and extracts the random_page_cost and seq_page_cost values from the tablespace options. If no tablespace-specific values are configured (options are NULL or values are negative), it falls back to the global configuration parameters random_page_cost and seq_page_cost.

The function is designed to be non-blocking and transaction-safe, meaning the returned values may change during query execution if tablespace parameters are modified concurrently. Both output parameters are optional - callers can pass NULL for either parameter they don't need.

The cost parameters are essential for PostgreSQL's cost-based query optimizer to make informed decisions about access methods, join algorithms, and overall query execution plans based on the expected performance characteristics of different storage systems.

## Parameters / Member Variables
- : The OID of the tablespace for which to retrieve page costs
- : Output parameter for random page access cost (can be NULL if not needed)  
- : Output parameter for sequential page access cost (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [get_tablespace](get_tablespace.md): Retrieve cached tablespace entry for the given OID
  - Assert: Debug assertion to ensure cache entry is valid
- Global variables referenced:
  - random_page_cost: Global default for random page access cost
  - seq_page_cost: Global default for sequential page access cost
- Data structures used:
  - TableSpaceCacheEntry: Cache entry containing tablespace options
  - TableSpaceOpts: Structure containing tablespace-specific cost parameters
- Called from:
  - [cost_seqscan](../c/cost_seqscan.md): Sequential scan cost estimation
  - [cost_samplescan](../c/cost_samplescan.md): Sample scan cost estimation  
  - [cost_index](../c/cost_index.md): Index scan cost estimation
  - [cost_bitmap_heap_scan](../c/cost_bitmap_heap_scan.md): Bitmap heap scan cost estimation
  - [cost_tidscan](../c/cost_tidscan.md): TID scan cost estimation
  - [cost_tidrangescan](../c/cost_tidrangescan.md): TID range scan cost estimation
  - [genericcostestimate](genericcostestimate.md): Generic index cost estimation
  - [gincostestimate](gincostestimate.md): GIN index cost estimation
  - [brincostestimate](../b/brincostestimate.md): BRIN index cost estimation

## Notes and Other Information
- This is a public function accessible throughout the PostgreSQL backend
- Values returned are not transaction-locked and may change during query execution
- Negative values in tablespace options indicate "use global default"
- Both output parameters are optional (can be NULL)
- Essential for cost-based query optimization across different storage systems
- Supports heterogeneous storage environments where different tablespaces have different I/O characteristics
- The function assumes the cache entry will always be valid (enforced by Assert)
- Part of PostgreSQL's sophisticated cost estimation framework used by the query planner