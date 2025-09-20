# gin_consistent_jsonb

## Location
[src/backend/utils/adt/jsonb_gin.c:929-1012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_gin.c#L929-L1012)

## Overview
The consistency check function for the JSONB GIN index implementation that determines whether an indexed tuple could match a given query based on the index keys present.

## Definition

```c
structure of the query
		 * object.  (Even if we could, we'd also have to worry about hashed
		 * keys and the index's failure to distinguish keys from string array
		 * elements.)  However, the tuple certainly doesn't match unless it
		 * contains all the query keys.
		 */
		*recheck = true;
```
## Detailed Description
This function implements the GIN consistency interface for JSONB data types. It evaluates whether a tuple from the index could potentially satisfy a query by examining which query keys are present in the tuple's index entries. The function handles multiple JSONB query strategies including containment (@>), existence (?), exists-any (?|), exists-all (?&), and JSONPath queries.

The function always sets the recheck flag to true because the GIN index alone cannot determine the exact structural relationships between keys and values in complex JSONB objects. The index can only confirm that certain keys or values are present, but cannot verify their hierarchical relationships or distinguish between object keys and array string elements.

## Parameters / Member Variables
- : Boolean array indicating which query keys are present in the index tuple
- : Strategy number indicating the type of JSONB query operation being performed
- : The JSONB query value (commented out parameter, not actively used)
- : Number of query keys to check
- : Additional data for complex queries (used for JSONPath operations)
- : Output parameter set to indicate whether the executor must recheck the match

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
  - JsonbExistsStrategyNumber
  - JsonbExistsAnyStrategyNumber
  - JsonbExistsAllStrategyNumber
  - JsonbJsonpathPredicateStrategyNumber
  - JsonbJsonpathExistsStrategyNumber
- Called from: GIN index access method during query execution

## Notes and Other Information
- Always requires recheck due to limitations of GIN indexing for complex JSON structures
- For containment queries (@>), all query keys must be present in the index for a potential match
- For existence queries (?, ?|, ?&), the function handles different logical combinations of key presence
- JSONPath queries use special execution logic via execute_jsp_gin_node function
- The function is located in src/backend/utils/adt/jsonb_gin.c:929-1012