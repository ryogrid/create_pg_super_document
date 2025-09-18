# has_unique_index

## Location
[src/backend/optimizer/util/plancat.c:2208-2239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L2208-L2239)

## Overview
Detects whether a unique index exists on a specified attribute of a relation, allowing the optimizer to conclude that all non-null values of the attribute are distinct.

## Definition
```c
bool has_unique_index(RelOptInfo *rel, AttrNumber attno)
```

## Detailed Description
The has_unique_index function is a utility used by PostgreSQL's query optimizer to determine whether a specific attribute (column) in a relation has a unique index that guarantees all non-null values in that column are distinct. This information is valuable for optimization decisions, particularly for statistical estimates and selectivity calculations.

The function iterates through all indexes associated with the given relation and checks for indexes that meet specific criteria:

1. **Uniqueness**: The index must be marked as unique
2. **Single Column**: The index must have exactly one key column (nkeycolumns == 1)
3. **Attribute Match**: The indexed column must match the specified attribute number
4. **Predicate Handling**: For partial indexes (those with WHERE clauses), the function only considers them valid if either there is no predicate (indpred == NIL) or the predicate is known to be satisfied by the current query (predOK is true)

The function explicitly excludes expressional indexes and multicolumn unique indexes, as these don't provide the guarantee that a single specified attribute is unique.

## Parameters / Member Variables
- `rel`: RelOptInfo structure containing information about the relation and its indexes
- `attno`: AttrNumber specifying the attribute (column) number to check for uniqueness

## Dependencies
- Functions called/Symbols referenced:
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure accessed)
  - lfirst (list iteration macro)
- Called from (representative examples):
  - examine_variable

## Notes and Other Information
This function does not check the index's indimmediate property, which means it may report uniqueness even when constraints could be temporarily violated within a transaction (deferred unique constraints). This behavior is appropriate for statistical estimation purposes but should not be relied upon for correctness proofs. The function is primarily used in selectivity estimation where the optimizer needs to know if an attribute's values are guaranteed to be unique for cardinality calculations.