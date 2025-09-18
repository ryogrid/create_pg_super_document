# set_hash_references

## Location
src/backend/optimizer/plan/setrefs.c: 1901 - 1933

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
  - build_tlist_index
  - fix_upper_expr
  - set_dummy_tlist_references
  - NUM_EXEC_QUAL
  - OUTER_VAR
  - NRM_EQUAL
  - Assert
- Called from (representative examples):
  - set_plan_refs
  - fix_scan_list

## Notes and Other Information
- This is a static function within setrefs.c for internal plan reference adjustment
- Hash nodes are typically the inner side of HashJoin operations
- The hashkeys expressions must reference the outer plan's target list, not scan relations
- Hash nodes don't have their own target list projection - they use dummy references
- Hash nodes don't evaluate qualifiers - they purely organize data for join operations
- The OUTER_VAR context ensures hashkeys reference the correct input tuples during hash table construction
- This function is simpler than other set_*_references functions because Hash nodes have a more limited role in query execution