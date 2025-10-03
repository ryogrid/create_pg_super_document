# is_usable_unique_index

## Location
[src/backend/commands/matview.c:898-951](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L898-L951)

## Overview
Determines whether a given index meets all the requirements to be used for materialized view match-merge refresh operations.

## Definition

```c
static bool
is_usable_unique_index(Relation indexRel)
```
## Detailed Description
This function validates that an index satisfies all the strict requirements needed for the sophisticated refresh_by_match_merge algorithm. The function performs comprehensive checks to ensure the index can reliably identify and match rows between the old and new versions of materialized view data.

The validation covers multiple aspects:
1. **Uniqueness**: The index must enforce uniqueness to guarantee unambiguous row identification
2. **Validity**: The index must be in a valid state (not broken or being built)
3. **Immediacy**: The index must enforce constraints immediately (not deferred)
4. **Completeness**: The index must not be partial (no WHERE clause)
5. **B-tree requirement**: Only B-tree indexes are supported due to equality operator requirements
6. **Column-based**: All indexed columns must be plain user columns, not expressions or system columns

## Parameters / Member Variables
- `indexRel`: The index relation to be evaluated for usability
## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_index (struct type)
  - [RelationGetIndexPredicate](../R/RelationGetIndexPredicate.md)
- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)
  - [refresh_by_match_merge](../r/refresh_by_match_merge.md)

## Notes and Other Information
- Returns true only if ALL requirements are met; any single failure results in false
- System columns (attnum <= 0) are explicitly rejected to ensure compatibility
- Index expressions are not supported because they cannot be reliably matched between old and new data
- The B-tree requirement ensures that appropriate equality operators exist and can be used in FULL JOIN operations
- This function is critical for determining whether concurrent refresh (match-merge) or blocking refresh (heap-swap) should be used
- Materialized views without usable unique indexes must use the heap-swap refresh method

## Simplified Source

```c
static bool
is_usable_unique_index(Relation indexRel)
{
    Form_pg_index indexStruct = indexRel->rd_index;

    // Check all required index properties for match-merge compatibility
    if (indexStruct->indisunique &&
        indexStruct->indimmediate &&
        indexRel->rd_rel->relam == BTREE_AM_OID &&
        indexStruct->indisvalid &&
        RelationGetIndexPredicate(indexRel) == NIL &&
        indexStruct->indnatts > 0)
    {
        // Verify all indexed columns are plain user columns (not expressions or system columns)
        for (int i = 0; i < indexStruct->indnatts; i++)
        {
            if (indexStruct->indkey.values[i] <= 0)
                return false;  // Reject system columns and expressions
        }
        return true;
    }
    return false;
}
```