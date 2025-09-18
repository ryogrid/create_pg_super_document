# _bt_keep_natts_fast

## Location
[src/backend/access/nbtree/nbtutils.c:4876-4922](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4876-L4922)

## Overview
A fast bitwise variant of _bt_keep_natts that performs inexpensive suffix truncation evaluation using naive bitwise datum comparisons to save cycles during B-tree split operations.

## Definition
```c
int _bt_keep_natts_fast(Relation rel, IndexTuple lastleft, IndexTuple firstright)
```

## Detailed Description
This function provides a performance-optimized approach to determining how many leading attributes should be kept when performing suffix truncation during B-tree operations. It uses bitwise equality comparisons instead of the more expensive opclass-specific comparisons used by _bt_keep_natts.

The function is specifically designed for scenarios where speed is more important than perfect accuracy, such as when evaluating candidate split points. It works by comparing attributes between the last tuple on the left page and the first tuple on the right page, determining how many leading attributes are identical.

The approach relies on the fact that most B-tree opclasses can only indicate two datums are equal if they are bitwise equal after detoasting. For indexes with only "equal image" columns, this routine is guaranteed to provide the same result as _bt_keep_natts.

## Parameters / Member Variables
- `rel`: The index relation being processed
- `lastleft`: The last tuple on the left page
- `firstright`: The first tuple on the right page

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetDescr (via TupleDesc access)
  - IndexRelationGetNumberOfKeyAttributes
  - [index_getattr](../i/index_getattr.md)
  - TupleDescAttr
  - [datum_image_eq](../d/datum_image_eq.md)
- Called from (representative examples):
  - [_bt_dedup_pass](_bt_dedup_pass.md)
  - [_bt_bottomupdel_pass](_bt_bottomupdel_pass.md)
  - _bt_do_singleval
  - _bt_load
  - [_bt_afternewitemoff](_bt_afternewitemoff.md)
  - [_bt_strategy](_bt_strategy.md)
  - [_bt_split_penalty](_bt_split_penalty.md)
  - [_bt_keep_natts](_bt_keep_natts.md)

## Notes and Other Information
- Exported function specifically designed for use by nbtsplitloc.c for evaluating split points
- Provides weaker guarantees than _bt_keep_natts but sufficient for most split location decisions
- False negatives generally only result in more balanced split points rather than correctness issues
- Starts with keepnatts = 1, meaning at least one attribute is always considered for truncation
- Breaks comparison loop on first difference in null status or bitwise content
- Performance optimization prioritizes speed over perfect accuracy in suffix truncation decisions