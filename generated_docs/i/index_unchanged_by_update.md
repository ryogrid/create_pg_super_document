# index_unchanged_by_update

## Location
[src/backend/executor/execIndexing.c:963-1076](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L963-L1076)

## Overview
Determines whether an index remains logically unchanged by an UPDATE operation, enabling optimization hints for index insertion operations by analyzing updated columns and index expressions.

## Definition

```c
static bool
index_unchanged_by_update(ResultRelInfo *resultRelInfo, EState *estate,
                          IndexInfo *indexInfo, Relation indexRelation)
```
## Detailed Description
This function performs a comprehensive analysis to determine if an index can be considered unchanged by an UPDATE operation. It implements a caching mechanism to avoid redundant calculations and follows a multi-step process:

1. **Cache Check**: First checks if the result has already been computed and cached in 
2. **Key Attribute Analysis**: Examines overlap between updated columns and the index's key attributes (excluding INCLUDE columns)
3. **Expression Analysis**: For indexes with expressions, uses a tree walker to detect if any variables in the expressions reference updated columns
4. **Optimization Decision**: Returns true if the index can be considered unchanged, enabling the  hint for performance optimization

The function specifically ignores INCLUDE columns (non-key columns) since changes to them don't affect the index's logical structure. It also deliberately ignores index predicates, even allowing the hint when partial indexes might not have corresponding tuples.

## Parameters / Member Variables
- `*resultRelInfo`: Information about the result relation being updated
- `*estate`: Executor state containing execution context and column update information
- `*indexInfo`: Metadata and state information about the specific index being analyzed
- `indexRelation`: The index relation object being checked
## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetUpdatedCols](../E/ExecGetUpdatedCols.md)
  - [ExecGetExtraUpdatedCols](../E/ExecGetExtraUpdatedCols.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_union](../b/bms_union.md)
  - [RelationGetIndexExpressions](../R/RelationGetIndexExpressions.md)
  - [index_expression_changed_walker](index_expression_changed_walker.md)
  - [list_free](../l/list_free.md)
  - [bms_free](../b/bms_free.md)
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md)

## Notes and Other Information
- This is a static function used internally within execIndexing.c for UPDATE optimization
- Implements caching via  and  fields to avoid repeated calculations
- Only considers key attributes, treating INCLUDE columns as opaque payload that doesn't affect index logical state
- Row-level BEFORE triggers don't affect the analysis since they don't modify the updatedCols bitmaps
- The function enables important performance optimizations by allowing index insertion code to skip unnecessary uniqueness checks when indexes are logically unchanged
- Deliberately ignores index predicates to provide hints even for partial indexes where the updated tuple might not have a corresponding index entry

## Simplified Source

```c
static bool
index_unchanged_by_update(ResultRelInfo *resultRelInfo, EState *estate,
                          IndexInfo *indexInfo, Relation indexRelation)
{
    Bitmapset  *updatedCols;
    Bitmapset  *extraUpdatedCols;
    Bitmapset  *allUpdatedCols;
    bool        hasexpression = false;
    List       *idxExprs;

    // Check cache first to avoid redundant computation
    if (indexInfo->ii_CheckedUnchanged)
        return indexInfo->ii_IndexUnchanged;
    indexInfo->ii_CheckedUnchanged = true;

    // Get updated column information
    updatedCols = ExecGetUpdatedCols(resultRelInfo, estate);
    extraUpdatedCols = ExecGetExtraUpdatedCols(resultRelInfo, estate);

    // Check key attributes for overlap with updated columns
    for (int attr = 0; attr < indexInfo->ii_NumIndexKeyAttrs; attr++) {
        int keycol = indexInfo->ii_IndexAttrNumbers[attr];

        if (keycol <= 0) {
            // Expression - will check later
            hasexpression = true;
            continue;
        }

        // Check if this key column was updated
        if (bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber, updatedCols) ||
            bms_is_member(keycol - FirstLowInvalidHeapAttributeNumber, extraUpdatedCols)) {
            // Changed key column - index is affected
            indexInfo->ii_IndexUnchanged = false;
            return false;
        }
    }

    // If no expressions, index is unchanged
    if (!hasexpression) {
        indexInfo->ii_IndexUnchanged = true;
        return true;
    }

    // Combine updated column bitmaps for expression analysis
    if (!extraUpdatedCols)
        allUpdatedCols = updatedCols;
    else
        allUpdatedCols = bms_union(updatedCols, extraUpdatedCols);

    // Check if indexed expressions reference updated columns
    idxExprs = RelationGetIndexExpressions(indexRelation);
    hasexpression = index_expression_changed_walker((Node *) idxExprs, allUpdatedCols);
    list_free(idxExprs);
    if (extraUpdatedCols)
        bms_free(allUpdatedCols);

    if (hasexpression) {
        indexInfo->ii_IndexUnchanged = false;
        return false;
    }

    // Index is unchanged - enable optimization hint
    indexInfo->ii_IndexUnchanged = true;
    return true;
}
```