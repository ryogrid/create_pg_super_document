# set_hash_references

## Location
[src/backend/optimizer/plan/setrefs.c:1901-1933](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1901-L1933)

## Overview
Adjusts variable references in a Hash plan node, specifically handling the hashkeys expressions that reference the outer plan's output during hash table construction.

## Definition
static void set_hash_references(PlannerInfo *root, Plan *plan, int rtoffset)

## Detailed Description
This function processes Hash plan nodes during the plan reference adjustment phase. Hash nodes are used as the inner side of hash join operations, building hash tables from their outer plan's tuple output. The function's primary responsibility is to adjust the hashkeys expressions to properly reference the outer plan's target list using OUTER_VAR references.

The function performs these specific operations:
1. Builds an indexed target list from the outer plan (lefttree) for efficient reference resolution
2. Adjusts the hashkeys expressions using fix_upper_expr() with OUTER_VAR context, ensuring they correctly reference columns from the outer plan's output
3. Sets up dummy target list references since Hash nodes don't project their own output
4. Asserts that Hash nodes have no qualifiers of their own

Hash nodes are unique in that they don't evaluate or project data - they simply organize incoming tuples from their outer plan into a hash table structure for efficient join processing.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The Hash plan node cast to Plan* (internally cast to Hash* for processing)
- : Range table offset to be applied to relation IDs and variable references

## Dependencies
- Functions called/Symbols referenced:
  - [build_tlist_index](../b/build_tlist_index.md)
  - [fix_upper_expr](../f/fix_upper_expr.md)
  - [set_dummy_tlist_references](set_dummy_tlist_references.md)
  - NUM_EXEC_QUAL
  - OUTER_VAR
  - NRM_EQUAL
  - Assert
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md)
  - fix_scan_list

## Notes and Other Information
- This is a static function within setrefs.c for internal plan reference adjustment
- [Hash](../H/Hash.md) nodes are typically the inner side of HashJoin operations
- The hashkeys expressions must reference the outer plan's target list, not scan relations
- [Hash](../H/Hash.md) nodes don't have their own target list projection - they use dummy references
- [Hash](../H/Hash.md) nodes don't evaluate qualifiers - they purely organize data for join operations
- The OUTER_VAR context ensures hashkeys reference the correct input tuples during hash table construction
- This function is simpler than other set_*_references functions because Hash nodes have a more limited role in query execution

## Simplified Source

```c
static void
set_hash_references(PlannerInfo *root, Plan *plan, int rtoffset) {
    Hash *hplan = (Hash *) plan;
    Plan *outer_plan = plan->lefttree;

    // Build index of outer plan's target list for efficient lookups
    indexed_tlist *outer_itlist = build_tlist_index(outer_plan->targetlist);

    // Adjust hashkeys to reference outer plan output using OUTER_VAR
    hplan->hashkeys = (List *) fix_upper_expr(root,
                                             (Node *) hplan->hashkeys,
                                             outer_itlist,
                                             OUTER_VAR,
                                             rtoffset,
                                             NRM_EQUAL,
                                             NUM_EXEC_QUAL(plan));

    // Hash doesn't project - use dummy target list references
    set_dummy_tlist_references(plan, rtoffset);

    // Hash nodes don't have their own qualifiers
    Assert(plan->qual == NIL);
}
```