# init_dummy_sjinfo

## Location
[src/backend/optimizer/path/joinrels.c:670-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L670-L704)

## Overview
Initializes a SpecialJoinInfo structure for a plain inner join between two specified sets of relations, providing minimal required information for join planning functions.

## Definition

```c
struct Relids set that identifies the joinrel (without OJ as yet). */
	joinrelids = bms_union(rel1->relids, rel2->relids);
```
## Detailed Description
The  function creates a minimal SpecialJoinInfo structure for inner joins. While inner joins normally don't require SpecialJoinInfo nodes (which are primarily used for outer joins, semijoins, and antijoins), some join planning functions need at least basic information about which relations are being joined.

The function populates the essential fields of the SpecialJoinInfo structure:
- Sets the join type to JOIN_INNER
- Establishes the left and right hand side relation sets
- Initializes commute relationship fields to NULL (no restrictions)
- Sets various join-specific flags to safe default values

This dummy SpecialJoinInfo can be used by cost estimation functions, join relation building, and other planning operations that require a consistent interface regardless of join type.

## Parameters / Member Variables
- : Pointer to the SpecialJoinInfo structure to be initialized
- : Bitmapset identifying the relations on the left side of the join
- : Bitmapset identifying the relations on the right side of the join

## Dependencies
- Functions called/Symbols referenced:
  - T_SpecialJoinInfo (node type)
  - JOIN_INNER (join type constant)
- Called from (representative examples):
  - [compute_semi_anti_join_factors](../c/compute_semi_anti_join_factors.md)
  - [approx_tuple_count](../a/approx_tuple_count.md)
  - [make_join_rel](../m/make_join_rel.md)
  - [build_child_join_sjinfo](../b/build_child_join_sjinfo.md)
  - [consider_new_or_clause](../c/consider_new_or_clause.md)

## Notes and Other Information
- Only populates essential fields needed for basic join planning operations
- Non-essential fields like , ,  are set to safe defaults
- The  field is set to 0 since inner joins don't create outer join relations
- Commute restriction lists are set to NULL, indicating no ordering restrictions
- This function enables consistent handling of all join types through the SpecialJoinInfo interface
- Widely used across different modules including cost estimation, join relation creation, and clause optimization

## Simplified Source

```c
void init_dummy_sjinfo(SpecialJoinInfo *sjinfo, Relids left_relids,
                       Relids right_relids)
{
    // Set up basic SpecialJoinInfo structure
    sjinfo->type = T_SpecialJoinInfo;

    // Assign relation sets for both syntactic and minimal requirements
    sjinfo->min_lefthand = left_relids;
    sjinfo->min_righthand = right_relids;
    sjinfo->syn_lefthand = left_relids;
    sjinfo->syn_righthand = right_relids;

    // Configure as inner join with no special properties
    sjinfo->jointype = JOIN_INNER;
    sjinfo->ojrelid = 0;  // No outer join relation ID for inner joins

    // No commutation restrictions for inner joins
    sjinfo->commute_above_l = NULL;
    sjinfo->commute_above_r = NULL;
    sjinfo->commute_below_l = NULL;
    sjinfo->commute_below_r = NULL;

    // Set remaining fields to safe defaults
    sjinfo->lhs_strict = false;
    sjinfo->semi_can_btree = false;
    sjinfo->semi_can_hash = false;
    sjinfo->semi_operators = NIL;
    sjinfo->semi_rhs_exprs = NIL;
}
```