# makeSortGroupClauseForSetOp

## Location
[src/backend/parser/analyze.c:1956-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/analyze.c#L1956-L2002)

## Overview
Creates a SortGroupClause node for SetOperationStmt's groupClauses, determining appropriate equality and sorting operators for set operation processing.

## Definition

```c
SortGroupClause *
makeSortGroupClauseForSetOp(Oid rescoltype, bool require_hash)
```
## Detailed Description
makeSortGroupClauseForSetOp is a utility function that constructs SortGroupClause nodes specifically for use in set operations (UNION, INTERSECT, EXCEPT). These clauses define how to compare rows for duplicate elimination and sorting purposes in set operations.

The function determines the appropriate equality and sorting operators for a given column type by calling get_sort_group_operators. It handles the special case where hash support is explicitly required by the caller - for record types (RECORDOID, RECORDARRAYOID), it assumes hash support is available even when the type cache indicates otherwise, since the caller may have domain-specific knowledge that hashing will work.

The resulting SortGroupClause has its tleSortGroupRef set to 0 initially since no target list exists yet at this stage of processing. The actual sort group reference will be assigned later during query transformation when the target list is available.

## Parameters / Member Variables
- : The OID of the result column type for which to create the SortGroupClause
- : Boolean flag indicating whether hash support is mandatory for this operation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (SortGroupClause creation)
  - [get_sort_group_operators](../g/get_sort_group_operators.md) (operator determination for sorting and equality)
- Called from (representative examples):
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (set operation tree processing)
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md) (recursive CTE processing)

## Notes and Other Information
- The function assumes hash support for record types when explicitly required, even if the type cache indicates otherwise
- The tleSortGroupRef field is initialized to 0 and must be set later when the actual target list is available
- The nulls_first field is set to false by default, which works whether or not a sort operator is available
- This function is specifically designed for set operations and may not be suitable for other sorting contexts
- Hash support determination is crucial for choosing between hash-based and sort-based set operation implementations
- The function handles both regular data types and complex types like records, providing flexibility for various set operation scenarios