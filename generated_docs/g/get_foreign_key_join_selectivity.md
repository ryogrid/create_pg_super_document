# get_foreign_key_join_selectivity

## Location
src/backend/optimizer/path/costsize.c: 5544 - 5794

## Overview
Analyzes join restriction clauses to identify those matching foreign key constraints and provides specialized selectivity estimates based on FK semantics rather than generic statistical methods.

## Definition
```c
static Selectivity get_foreign_key_join_selectivity(PlannerInfo *root,
                                                    Relids outer_relids, Relids inner_relids,
                                                    SpecialJoinInfo *sjinfo, List **restrictlist)
```

## Detailed Description
This sophisticated static function leverages foreign key constraint knowledge to provide more accurate join selectivity estimates than generic statistical methods. It operates by identifying join clauses that correspond to foreign key relationships and computing selectivity based on the fundamental FK property: each referencing row matches exactly one row in the referenced table.

The function performs several key operations:

1. **FK Constraint Matching**: Iterates through known foreign key constraints (`root->fkey_list`) to find those connecting the current join's input relations.

2. **Clause Identification and Removal**: Identifies restriction clauses that match FK columns through either:
   - Equivalence class membership for EC-derived clauses
   - Direct matching for "loose" clauses previously associated with the FK
   
3. **Special Join Handling**: Implements specific logic for semi/anti joins, where FK knowledge helps determine the fraction of outer rows with matches.

4. **Selectivity Calculation**: Computes selectivity using FK semantics:
   - For regular joins: `1.0 / referenced_table_size`
   - For semi/anti joins: `referenced_table_filtered_rows / referenced_table_total_rows`

5. **Constant Correction**: Adjusts estimates when FK columns participate in constant equality constraints to avoid double-counting selectivity.

6. **Safety Checks**: Validates that all expected matching clauses were found before applying FK selectivity, reverting to generic estimation if validation fails.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning context and FK constraint list
- `outer_relids`: Bitmap identifying relations on the outer side of the join
- `inner_relids`: Bitmap identifying relations on the inner side of the join  
- `sjinfo`: SpecialJoinInfo describing join type and constraints
- `restrictlist`: Pointer to list of restriction clauses (modified by removing FK-matched clauses)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md), bms_membership
  - [list_copy](../l/list_copy.md), list_member_ptr, list_concat
  - foreach_delete_current
  - [find_base_rel](../f/find_base_rel.md)
  - [find_derived_clause_for_ec_member](../f/find_derived_clause_for_ec_member.md)
  - [clause_selectivity](../c/clause_selectivity.md)
  - CLAMP_PROBABILITY
  - Types: ForeignKeyOptInfo, EquivalenceClass, EquivalenceMember
  - Constants: JOIN_SEMI, JOIN_ANTI, BMS_SINGLETON
- Called from (representative examples):
  - [calc_joinrel_size_estimate](../c/calc_joinrel_size_estimate.md) (src/backend/optimizer/path/costsize.c:5424)

## Notes and Other Information
- This is a static function accessible only within costsize.c
- Provides significant estimation improvements for multi-column FKs where independence assumptions fail
- Handles both equivalence class-derived and "loose" join clauses  
- Includes sophisticated logic to avoid double-counting selectivity when FKs overlap with constant constraints
- Does not currently adjust for null values in referencing columns, acknowledging this as a known limitation
- Assumes FK constraints apply uniformly across inheritance hierarchies
- Returns 1.0 when no applicable FK constraints are found, allowing normal selectivity estimation to proceed
- The function modifies the input restrictlist by removing FK-matched clauses