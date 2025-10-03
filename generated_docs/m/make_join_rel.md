# make_join_rel

## Location
[src/backend/optimizer/path/joinrels.c:705-801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L705-L801)

## Overview
Creates or finds a join RelOptInfo representing the join of two relations, adding path information for joins with the two relations as outer and inner.

## Definition

```c
struct Relids set that identifies the joinrel (without OJ as yet). */
	joinrelids = bms_union(rel1->relids, rel2->relids);
```
## Detailed Description
The  function is a core component of PostgreSQL's query optimizer that handles the creation of join relations. It takes two relations and attempts to create a valid join between them, considering various join types including outer joins, and constraints from IN or EXISTS clauses that have been converted to joins. The function performs validity checks, determines the appropriate join type, constructs the canonical relation identifiers including any outer join dependencies, and populates the resulting join relation with possible execution paths.

The function can return NULL if the attempted join is not valid, which commonly occurs when working with outer joins or with complex subquery constructs. It ensures that no overlapping relation sets are joined and handles the complexity of outer join ordering requirements.

## Parameters / Member Variables
- : The PlannerInfo structure containing global information about the query being planned
- : The first RelOptInfo representing one of the relations to be joined  
- : The second RelOptInfo representing the other relation to be joined

## Dependencies
- Functions called/Symbols referenced:
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_union](../b/bms_union.md)
  - [join_is_legal](../j/join_is_legal.md)
  - [add_outer_joins_to_relids](../a/add_outer_joins_to_relids.md)
  - [init_dummy_sjinfo](../i/init_dummy_sjinfo.md)
  - [build_join_rel](../b/build_join_rel.md)
  - [is_dummy_rel](../i/is_dummy_rel.md)
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [join_search_one_level](../j/join_search_one_level.md)
  - [make_rels_by_clause_joins](make_rels_by_clause_joins.md)
  - [make_rels_by_clauseless_joins](make_rels_by_clauseless_joins.md)
  - [merge_clump](merge_clump.md)

## Notes and Other Information
- The function includes an assertion to verify that the two input relations do not have overlapping base relation sets
- Returns NULL for invalid joins, particularly important for outer join constraints
- Handles relation swapping when needed to match join information requirements
- Creates dummy SpecialJoinInfo for plain inner joins when no specific join information exists
- The resulting join relation may already contain paths from other relation pairs that form the same base relation set
- Memory management is handled through bms_free calls for temporary bitmap sets

## Simplified Source

```c
RelOptInfo *
make_join_rel(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2)
{
    Relids joinrelids;
    SpecialJoinInfo *sjinfo;
    bool reversed;
    List *pushed_down_joins = NIL;
    SpecialJoinInfo sjinfo_data;
    RelOptInfo *joinrel;
    List *restrictlist;

    // Verify relations don't overlap
    Assert(!bms_overlap(rel1->relids, rel2->relids));

    // Construct basic join relids
    joinrelids = bms_union(rel1->relids, rel2->relids);

    // Check if join is legal and determine join type
    if (!join_is_legal(root, rel1, rel2, joinrelids, &sjinfo, &reversed))
    {
        bms_free(joinrelids);
        return NULL;  // Invalid join
    }

    // Add outer join dependencies to canonical relids
    joinrelids = add_outer_joins_to_relids(root, joinrelids, sjinfo, &pushed_down_joins);

    // Swap relations if needed to match join info
    if (reversed)
    {
        RelOptInfo *temp = rel1;
        rel1 = rel2;
        rel2 = temp;
    }

    // Create dummy SpecialJoinInfo for plain inner joins
    if (sjinfo == NULL)
    {
        sjinfo = &sjinfo_data;
        init_dummy_sjinfo(sjinfo, rel1->relids, rel2->relids);
    }

    // Build the join relation and get restriction clauses
    joinrel = build_join_rel(root, joinrelids, rel1, rel2, sjinfo, pushed_down_joins, &restrictlist);

    // Skip path generation for dummy relations
    if (is_dummy_rel(joinrel))
    {
        bms_free(joinrelids);
        return joinrel;
    }

    // Generate all possible join paths
    populate_joinrel_with_paths(root, rel1, rel2, joinrel, sjinfo, restrictlist);

    bms_free(joinrelids);
    return joinrel;
}
```