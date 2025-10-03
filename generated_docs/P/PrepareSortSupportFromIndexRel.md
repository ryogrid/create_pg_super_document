# PrepareSortSupportFromIndexRel

## Location
[src/backend/utils/sort/sortsupport.c:161-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sortsupport.c#L161-L187)

## Overview
Sets up a SortSupport structure using information from an index relation and a specified strategy to configure sorting for index-related operations.

## Definition

```c
void
PrepareSortSupportFromIndexRel(Relation indexRel, int16 strategy,
							   SortSupport ssup)
```
## Detailed Description
PrepareSortSupportFromIndexRel configures sort support functionality specifically for index-based sorting operations. The function:

1. **Extracts operator information**: Retrieves the operator family (opfamily) and input type (opcintype) from the index relation's metadata for the specified attribute
2. **Validates the index type**: Ensures the relation uses a B-tree access method, as this function is specifically designed for B-tree indexes
3. **Validates the strategy**: Confirms the strategy is either BTLessStrategyNumber or BTGreaterStrategyNumber
4. **Sets sort direction**: Configures ssup_reverse based on whether the strategy indicates descending order (BTGreaterStrategyNumber)
5. **Configures the comparator**: Delegates to FinishSortSupportFunction to set up the actual comparison function

This function is primarily used in contexts where sorting needs to match the ordering defined by an existing B-tree index, such as during index builds, cluster operations, or when leveraging index ordering for query execution.

## Parameters / Member Variables
- `indexRel`: The index relation containing operator family and type information
- `strategy`: B-tree strategy number (BTLessStrategyNumber for ascending, BTGreaterStrategyNumber for descending)
- `ssup`: SortSupport structure to be configured (must be pre-initialized with context, attribute number, collation, and nulls handling)
## Dependencies
- Functions called/Symbols referenced:
  - [SortSupport](../S/SortSupport.md) (type)
  - BTGreaterStrategyNumber
  - BTLessStrategyNumber
  - [FinishSortSupportFunction](../F/FinishSortSupportFunction.md)
- Called from:
  - [_bt_load](../b/_bt_load.md) (at src/backend/access/nbtree/nbtsort.c:1187)
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md) (at src/backend/utils/sort/tuplesortvariants.c:341)
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md) (at src/backend/utils/sort/tuplesortvariants.c:426)
  - [ApplySortAbbrevFullComparator](../A/ApplySortAbbrevFullComparator.md) (at src/include/utils/sortsupport.h:387)

## Notes and Other Information
- This is a public function, part of PostgreSQL's sort support API
- Specifically designed for B-tree indexes and will error if used with other access methods
- Used primarily in index-related operations like index builds and cluster commands
- The caller must pre-initialize the SortSupport structure with ssup_cxt, ssup_attno, ssup_collation, and ssup_nulls_first
- Ensures consistency between sort operations and existing index definitions
- Strategy validation prevents misuse with invalid B-tree strategy numbers
- Essential for operations that need to maintain or replicate the sort order of existing B-tree indexes

## Simplified Source

```c
void
PrepareSortSupportFromIndexRel(Relation indexRel, int16 strategy,
                              SortSupport ssup)
{
    Oid opfamily = indexRel->rd_opfamily[ssup->ssup_attno - 1];
    Oid opcintype = indexRel->rd_opcintype[ssup->ssup_attno - 1];

    // Validate this is a B-tree index
    if (indexRel->rd_rel->relam != BTREE_AM_OID)
        elog(ERROR, "unexpected non-btree AM: %u", indexRel->rd_rel->relam);

    // Validate strategy is valid for B-tree
    if (strategy != BTGreaterStrategyNumber &&
        strategy != BTLessStrategyNumber)
        elog(ERROR, "unexpected sort support strategy: %d", strategy);

    // Set sort direction based on strategy
    ssup->ssup_reverse = (strategy == BTGreaterStrategyNumber);

    // Configure the actual comparator function
    FinishSortSupportFunction(opfamily, opcintype, ssup);
}
```