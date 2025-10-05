# _bt_check_third_page

## Location
[src/backend/access/nbtree/nbtutils.c:5083-5140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L5083-L5140)

## Overview
Validates that a new index tuple can fit on a B-tree page by enforcing the constraint that any single item must not exceed 1/3 of the available page space.

## Definition
```c
void _bt_check_third_page(Relation rel, Relation heap, bool needheaptidspace, Page page, IndexTuple newtup)
```

## Detailed Description
This function enforces the fundamental B-tree constraint that every page must be able to accommodate at least three items. By restricting any single item to 1/3 of the available page space, it ensures that page splits can always create valid pages with sufficient items.

The function performs a two-tier size check:
1. First checks against the standard BTMaxItemSize limit
2. If that fails but needheaptidspace is false, checks against BTMaxItemSizeNoHeapTid limit (for version 2/3 indexes or internal pages)

For internal pages, oversized tuples indicate a serious inconsistency since leaf-level insertions should have caught the problem earlier. For leaf pages, the function provides detailed error messages with suggestions for resolving large value issues.

## Parameters / Member Variables
- `rel`: The index relation where the tuple will be inserted
- `heap`: The heap relation that the index tuple references (for error reporting)
- `needheaptidspace`: Whether the tuple requires space for a heap TID (affects size limits)
- `page`: The target page where the tuple will be inserted
- `newtup`: The new index tuple to be validated for size

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize
  - MAXALIGN
  - BTMaxItemSize
  - BTMaxItemSizeNoHeapTid
  - BTPageGetOpaque
  - P_ISLEAF
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - RelationGetRelationName
  - ereport/elog (error reporting)
  - Constants: BTREE_VERSION, BTREE_NOVAC_VERSION
- Called from (representative examples):
  - [_bt_findinsertloc](_bt_findinsertloc.md)
  - [_bt_buildadd](_bt_buildadd.md)

## Notes and Other Information
- Enforces the "rule of three" - every B-tree page must accommodate at least three items
- Item size calculation includes MAXALIGN overhead but excludes ItemId overhead
- Different size limits apply based on B-tree version and whether heap TID space is needed
- Internal page size violations indicate serious consistency problems (should never happen)
- Error messages suggest alternatives: MD5 hash function indexes or full-text indexing
- TOAST methods are not applied here as they would break suffix truncation and amcheck assumptions
- Provides detailed error context including tuple location in heap relation
- Size limits vary between versions: higher limits for version 2/3 indexes and internal pages

## Simplified Source

```c
void _bt_check_third_page(Relation rel, Relation heap, bool needheaptidspace,
                          Page page, IndexTuple newtup)
{
    Size itemsz;
    BTPageOpaque opaque;

    // Calculate aligned item size
    itemsz = MAXALIGN(IndexTupleSize(newtup));

    // Check against standard size limit
    if (itemsz <= BTMaxItemSize(page))
        return;

    // Check against relaxed limit for version 2/3 indexes or internal pages
    if (!needheaptidspace && itemsz <= BTMaxItemSizeNoHeapTid(page))
        return;

    // Internal pages should never have oversized tuples
    opaque = BTPageGetOpaque(page);
    if (!P_ISLEAF(opaque))
        elog(ERROR, "cannot insert oversized tuple of size %zu on internal page of index \"%s\"",
             itemsz, RelationGetRelationName(rel));

    // Report detailed error for leaf page size violation
    ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("index row size %zu exceeds btree version %u maximum %zu for index \"%s\"",
                    itemsz,
                    needheaptidspace ? BTREE_VERSION : BTREE_NOVAC_VERSION,
                    needheaptidspace ? BTMaxItemSize(page) : BTMaxItemSizeNoHeapTid(page),
                    RelationGetRelationName(rel)),
             errdetail("Index row references tuple (%u,%u) in relation \"%s\".",
                      ItemPointerGetBlockNumber(BTreeTupleGetHeapTID(newtup)),
                      ItemPointerGetOffsetNumber(BTreeTupleGetHeapTID(newtup)),
                      RelationGetRelationName(heap)),
             errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
                    "Consider a function index of an MD5 hash of the value, "
                    "or use full text indexing."),
             errtableconstraint(heap, RelationGetRelationName(rel))));
}
```