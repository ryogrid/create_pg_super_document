# gin_consistent_jsonb_path

## Location
[src/backend/utils/adt/jsonb_gin.c:1220-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L1220-L1271)

## Overview
The consistency check function for the jsonb_path_ops GIN opclass that determines whether an indexed tuple could match a query using path-sensitive hash comparisons.

## Definition
```c
Datum gin_consistent_jsonb_path(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the GIN consistency interface specifically for the jsonb_path_ops opclass. Unlike the standard jsonb_ops consistency function, this version works with path-sensitive hashes that incorporate both JSON values and their hierarchical key paths. The function handles containment queries (@>) and JSONPath operations, but is inherently more restrictive than the standard jsonb_ops approach.

The jsonb_path_ops approach is necessarily lossy due to hash collisions and the incomplete preservation of JSON structure information in the index. Additionally, certain containment semantics (particularly around raw scalars in arrays) require full tuple examination. Therefore, the function always sets the recheck flag, but can definitively eliminate non-matches when required keys are absent from the index.

## Parameters / Member Variables
- `check`: Boolean array indicating which query keys are present in the indexed tuple
- `strategy`: Strategy number indicating the query operation type
- `query`: The query value (commented out parameter, not actively used)
- `nkeys`: Number of query keys to evaluate
- `extra_data`: Additional data for complex queries (used for JSONPath operations)
- `recheck`: Output parameter always set to true to force executor recheck

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER
  - PG_GETARG_UINT16
  - PG_GETARG_INT32
  - [execute_jsp_gin_node](../e/execute_jsp_gin_node.md)
  - PG_RETURN_BOOL
  - elog
- Strategy constants:
  - JsonbContainsStrategyNumber
  - JsonbJsonpathPredicateStrategyNumber
  - JsonbJsonpathExistsStrategyNumber
- Types and constants:
  - JsonPathGinNode
  - GIN_FALSE
- Called from: GIN index access method during query execution

## Notes and Other Information
- Designed exclusively for jsonb_path_ops opclass, providing better selectivity than jsonb_ops
- Always requires recheck due to inherent lossiness of path-sensitive hashing approach
- Hash collisions and incomplete structural information necessitate executor-level verification
- For containment queries, returns false only when required keys are definitively absent
- Supports fewer query types than standard jsonb_ops (only @> and JSONPath operations)
- More efficient for supported query types due to better index selectivity
- Special containment rules for raw scalars in arrays are not handled at index level
- Located in src/backend/utils/adt/jsonb_gin.c:1220-1271