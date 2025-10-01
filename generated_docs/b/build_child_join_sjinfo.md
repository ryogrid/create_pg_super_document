# build_child_join_sjinfo

## Location
[src/backend/optimizer/path/joinrels.c:1694-1747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1694-L1747)

## Overview
Constructs a SpecialJoinInfo structure for a child join by translating the SpecialJoinInfo from the parent join between partitioned relations.

## Definition
static SpecialJoinInfo *build_child_join_sjinfo(PlannerInfo *root, SpecialJoinInfo *parent_sjinfo, Relids left_relids, Relids right_relids)

## Detailed Description
This function creates a SpecialJoinInfo structure for a child partition join by adapting the parent join's SpecialJoinInfo structure. The translation process involves adjusting relation identifiers and expressions to reference the appropriate child partitions instead of their parent relations.

For INNER joins, the function creates a dummy SpecialJoinInfo since inner joins don't require complex outer join tracking. For other join types, it performs a deep translation by:

1. Copying the parent SpecialJoinInfo structure as a starting point
2. Finding AppendRelInfo structures for both left and right child relations
3. Adjusting various relation ID sets (min_lefthand, min_righthand, syn_lefthand, syn_righthand) to reference child partitions
4. Translating semi-join right-hand side expressions using adjust_appendrel_attrs
5. Cleaning up temporary AppendRelInfo arrays

The function maintains the semantic correctness of join operations while ensuring that all references point to the appropriate child partition relations.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and context information
- : The SpecialJoinInfo structure from the parent join operation to be translated
- : Bitmapset of relation IDs for the left side of the child join
- : Bitmapset of relation IDs for the right side of the child join

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [init_dummy_sjinfo](../i/init_dummy_sjinfo.md)
  - [find_appinfos_by_relids](../f/find_appinfos_by_relids.md)
  - [adjust_child_relids](../a/adjust_child_relids.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - memcpy
  - [pfree](../p/pfree.md)
  - JOIN_INNER
- Called from (representative examples):
  - [try_partitionwise_join](../t/try_partitionwise_join.md)

## Notes and Other Information
- Creates dummy SpecialJoinInfo for INNER joins since they don't require outer join tracking
- The outer-join relids (ojrelid) do not need adjustment during translation
- Maintains correspondence with free_child_join_sjinfo() - any changes to translation logic should be reflected in the cleanup function
- Essential for maintaining correct join semantics in partitionwise join optimization
- Memory management includes cleanup of temporary AppendRelInfo arrays after translation

## Simplified Source

```c
static SpecialJoinInfo *build_child_join_sjinfo(PlannerInfo *root,
                                               SpecialJoinInfo *parent_sjinfo,
                                               Relids left_relids,
                                               Relids right_relids) {
    SpecialJoinInfo *sjinfo = makeNode(SpecialJoinInfo);

    // Handle simple INNER joins - create dummy SpecialJoinInfo
    if (parent_sjinfo->jointype == JOIN_INNER) {
        Assert(parent_sjinfo->ojrelid == 0);
        init_dummy_sjinfo(sjinfo, left_relids, right_relids);
        return sjinfo;
    }

    // Copy parent SpecialJoinInfo as starting point
    memcpy(sjinfo, parent_sjinfo, sizeof(SpecialJoinInfo));

    // Find AppendRelInfo structures for child relations
    AppendRelInfo **left_appinfos, **right_appinfos;
    int left_nappinfos, right_nappinfos;

    left_appinfos = find_appinfos_by_relids(root, left_relids, &left_nappinfos);
    right_appinfos = find_appinfos_by_relids(root, right_relids, &right_nappinfos);

    // Translate relation ID sets to reference child partitions
    sjinfo->min_lefthand = adjust_child_relids(sjinfo->min_lefthand,
                                              left_nappinfos, left_appinfos);
    sjinfo->min_righthand = adjust_child_relids(sjinfo->min_righthand,
                                               right_nappinfos, right_appinfos);
    sjinfo->syn_lefthand = adjust_child_relids(sjinfo->syn_lefthand,
                                              left_nappinfos, left_appinfos);
    sjinfo->syn_righthand = adjust_child_relids(sjinfo->syn_righthand,
                                               right_nappinfos, right_appinfos);

    // Translate semi-join expressions for child relations
    sjinfo->semi_rhs_exprs = (List *) adjust_appendrel_attrs(root,
                                                            (Node *) sjinfo->semi_rhs_exprs,
                                                            right_nappinfos,
                                                            right_appinfos);

    // Clean up temporary arrays
    pfree(left_appinfos);
    pfree(right_appinfos);

    return sjinfo;
}
```