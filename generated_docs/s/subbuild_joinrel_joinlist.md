# subbuild_joinrel_joinlist

## Location
[src/backend/optimizer/util/relnode.c:1418-1469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1418-L1469)

## Overview
Processes a joininfo list to extract clauses that remain as join clauses at the current join level, filtering out those that become restriction clauses.

## Definition

```c
static List *
subbuild_joinrel_joinlist(RelOptInfo *joinrel,
						  List *joininfo_list,
						  List *new_joininfo)
```
## Detailed Description
The `subbuild_joinrel_joinlist` function examines each clause in the input joininfo_list and determines whether it should remain as a join clause at the current join level. Clauses that refer only to relations within the joinrel become restriction clauses and are ignored by this function. Clauses that still reference outside relations remain as join clauses and are added to the new_joininfo list.

The function is specifically designed to work with join relations (RELOPT_JOINREL) and carefully eliminates duplicates using pointer equality, since RestrictInfo nodes are multiply-linked rather than copied across different joininfo lists.

## Parameters / Member Variables
- `joinrel`: The join relation being constructed, used to determine which clauses become restriction clauses
- `joininfo_list`: Input list of joininfo clauses to be processed
- `new_joininfo`: Existing joininfo list to which qualifying join clauses will be appended

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_JOINREL
  - [bms_is_subset](../b/bms_is_subset.md)
  - [list_append_unique_ptr](../l/list_append_unique_ptr.md)
- Called from (representative examples):
  - [build_joinrel_joinlist](../b/build_joinrel_joinlist.md)

## Notes and Other Information
- This is a static function within relnode.c, used internally for join relation construction
- The function asserts that it should only be called for join relations (RELOPT_JOINREL)
- Clauses that become restriction clauses are ignored since they will be handled by subbuild_joinrel_restrictlist
- Duplicate elimination uses pointer equality since RestrictInfo nodes are multiply-linked
- The function operates at lines 1418-1469 in src/backend/optimizer/util/relnode.c
- This function is the counterpart to subbuild_joinrel_restrictlist, handling the join clauses while the other handles restriction clauses