# free_child_join_sjinfo

## Location
[src/backend/optimizer/path/joinrels.c:1748-1789](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1748-L1789)

## Overview
Frees memory consumed by a SpecialJoinInfo structure that was created by build_child_join_sjinfo().

## Definition
static void free_child_join_sjinfo(SpecialJoinInfo *child_sjinfo, SpecialJoinInfo *parent_sjinfo)

## Detailed Description
This function performs selective memory cleanup for a child join's SpecialJoinInfo structure. It carefully manages memory deallocation by only freeing fields that were translated copies created during the build_child_join_sjinfo() process, while preserving shared references to the parent's data structures.

The function handles two distinct cases:
1. **INNER joins**: No cleanup needed since dummy SpecialJoinInfos don't have translated fields
2. **Other join types**: Selective cleanup of relation ID bitmapsets that were copied and modified during translation

For non-inner joins, the function:
- Compares each relid bitmapset (min_lefthand, min_righthand, syn_lefthand, syn_righthand) with the parent's version
- Only frees bitmapsets that differ from the parent (indicating they were translated copies)
- Verifies through assertions that certain fields remain shared with the parent
- Leaves semi_rhs_exprs intact since simple pfree() is insufficient for expression trees

This selective approach prevents double-free errors and memory corruption while ensuring proper cleanup of translated data structures.

## Parameters / Member Variables
- : The child SpecialJoinInfo structure to be freed
- : The original parent SpecialJoinInfo structure used for comparison to determine which fields were translated copies

## Dependencies
- Functions called/Symbols referenced:
  - [bms_free](../b/bms_free.md)
  - [pfree](../p/pfree.md)
  - JOIN_INNER
  - Assert
- Called from (representative examples):
  - [try_partitionwise_join](../t/try_partitionwise_join.md)

## Notes and Other Information
- Only frees translated copies of fields, not shared references to parent data
- Corresponds directly to build_child_join_sjinfo() - changes to translation logic should be reflected here
- Uses pointer comparison to determine which bitmapsets were copied during translation
- semi_rhs_exprs are intentionally not freed due to complexity of expression tree deallocation
- Includes assertions to verify that commutation flags and semi_operators remain shared with parent
- Essential for preventing memory leaks in partitionwise join processing without causing double-free errors