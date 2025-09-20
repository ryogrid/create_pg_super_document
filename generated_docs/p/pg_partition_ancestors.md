# pg_partition_ancestors

## Location
[src/backend/utils/adt/partitionfuncs.c:201-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/partitionfuncs.c#L201-L238)

## Overview
A PostgreSQL system function that returns a set of relation OIDs representing all ancestors in a partition hierarchy, including the input relation itself, implemented as a set-returning function (SRF).

## Definition

```c
Datum
pg_partition_ancestors(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the  SQL function that provides a complete view of a partition hierarchy from a given relation up to the root partitioned table. Unlike  which returns only the topmost parent, this function returns all intermediate ancestors, making it valuable for comprehensive partition tree analysis.

The function operates as a set-returning function (SRF) using PostgreSQL's SRF framework:
1. **First Call**: Validates the input relation, retrieves ancestors, and prepares the result set
2. **Subsequent Calls**: Returns one ancestor OID per call until all ancestors are returned
3. **Final Call**: Cleans up and signals completion

The returned set includes the input relation itself as the first element, followed by its immediate parent, grandparent, and so on up to the root partitioned table.

## Parameters / Member Variables
- Input parameter (accessed via ):
  - : OID of the relation for which to retrieve all partition ancestors
- Internal state variables:
  - : Function call context for managing SRF state
  - : List of ancestor OIDs maintained across function calls

## Dependencies
- Functions called/Symbols referenced:
  - [check_rel_can_be_partition](../c/check_rel_can_be_partition.md): Validates if the relation can participate in partition trees
  - [get_partition_ancestors](../g/get_partition_ancestors.md): Retrieves the list of ancestor relations
  - [lcons_oid](../l/lcons_oid.md): Prepends the input relation OID to the ancestors list
  - SRF_IS_FIRSTCALL: Checks if this is the first call in the SRF sequence
  - SRF_FIRSTCALL_INIT: Initializes the function call context for SRF
  - SRF_PERCALL_SETUP: Sets up context for each subsequent call
  - SRF_RETURN_NEXT: Returns the next value in the result set
  - SRF_RETURN_DONE: Signals completion of the result set
  - list_length: Gets the number of elements in the ancestors list
  - [list_nth_oid](../l/list_nth_oid.md): Retrieves the OID at a specific position in the list

- Called from (representative examples):
  - Available as SQL function  for end-user queries
  - Used by partition management and introspection tools

## Notes and Other Information
- Implemented as a set-returning function (SRF) allowing it to return multiple rows from a single function call
- Uses PostgreSQL's multi-call function context to maintain state between calls
- Memory management is handled through PostgreSQL's memory context system
- Returns an empty set (no rows) for relations that cannot be part of partition trees
- The input relation is always included as the first result, even if it has no ancestors
- Efficient implementation that leverages existing partition hierarchy traversal functions
- Critical for SQL queries that need to analyze entire partition hierarchies
- Supports LATERAL joins and other advanced SQL patterns for partition analysis
- Part of PostgreSQL's comprehensive partition introspection API
- Particularly useful for administrative scripts and partition maintenance operations