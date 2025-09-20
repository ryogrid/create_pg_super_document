# mark_rels_nulled_by_join

## Location
[src/backend/optimizer/plan/initsplan.c:1322-1359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1322-L1359)

## Overview
Marks base relations that are nulled by an outer join by updating their nulling_relids field in the RelOptInfo structure.

## Definition

```c
static void
mark_rels_nulled_by_join(PlannerInfo *root, Index ojrelid,
						 Relids lower_rels)
```
## Detailed Description
The  function processes relations that can be nulled by an outer join operation. It iterates through all relations specified in the  bitmap and marks each base relation by adding the outer join's relation ID to their  field. This information is crucial for the optimizer to understand which relations might produce NULL values due to outer join semantics.

The function handles only base relations (not outer joins themselves) and updates their RelOptInfo structures to track which outer joins can null their tuples. This metadata is essential for correct query optimization, particularly for understanding when expressions involving these relations might evaluate to NULL.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : Range table index of the outer join RTE that performs the nulling (must not be 0)
- : Bitmap of base relation and outer join Relids that are syntactically below the nullable side of the join

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (struct)
- Called from (representative examples):
  - [deconstruct_recurse](../d/deconstruct_recurse.md) (multiple times)

## Notes and Other Information
- The function only processes actual base relations, skipping over outer join relations found in the lower_rels set
- An assertion verifies that any NULL RelOptInfo entries correspond to outer joins that are properly tracked in root->outer_join_rels
- The nulling_relids information is used throughout the optimizer to track which relations might produce NULL values
- This function is typically called during the deconstruction of the join tree to properly annotate relations with their nulling dependencies