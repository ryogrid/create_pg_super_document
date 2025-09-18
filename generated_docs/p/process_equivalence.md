# process_equivalence

## Location
src/backend/optimizer/path/equivclass.c: 117 - 470

## Overview
Processes equivalence clauses with mergejoinable operators to build or update EquivalenceClass structures that represent transitively equal expressions in the query optimizer.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's query optimizer equivalence class system. It analyzes equality clauses (e.g., a.col = b.col) and determines if they can be represented as EquivalenceClasses, which enable the optimizer to understand that certain expressions are transitively equal and can be used interchangeably for optimization purposes.

The function implements a UNION-FIND-like algorithm to manage equivalence relationships. It handles four main scenarios when processing a new equivalence clause:
1. Both expressions already exist in the same EquivalenceClass (no action needed)
2. Both expressions exist in different EquivalenceClasses (merge the classes)
3. One expression exists in an EquivalenceClass (add the other to the same class)
4. Neither expression exists (create a new EquivalenceClass)

The function also performs important transformations, such as converting X=X clauses into IS NOT NULL tests when the operator is strict, which provides better selectivity estimates.

Security considerations are built in - the function rejects clauses containing leaky functions when security_level > 0 to ensure proper evaluation timing.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and equivalence class lists
- : Pointer to RestrictInfo representing the equality clause being processed; may be modified to point to a transformed clause
- : JoinDomain limiting the applicability of deductions from the EquivalenceClass

## Dependencies
- Functions called/Symbols referenced:
  - [canonicalize_ec_expression](../c/canonicalize_ec_expression.md)
  - [add_eq_member](../a/add_eq_member.md)
  - [is_opclause](../i/is_opclause.md)
  - [get_leftop](../g/get_leftop.md)/get_rightop
  - [op_input_types](../o/op_input_types.md)
  - [equal](../e/equal.md)
  - set_opfuncid
  - [func_strict](../f/func_strict.md)
  - [make_restrictinfo](../m/make_restrictinfo.md)
  - [list_concat](../l/list_concat.md)
  - [bms_join](../b/bms_join.md)
- Called from (representative examples):
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md)
  - [reconsider_full_join_clause](../r/reconsider_full_join_clause.md)

## Notes and Other Information
- Only called during planner startup, not during GEQO exploration
- Implements equivalence class merging with proper handling of canonical state constraints
- Returns true if the clause was successfully processed as an equivalence, false if it should be treated as an ordinary restriction clause
- Initializes left_ec/right_ec fields in the RestrictInfo to point to the representing EquivalenceClass
- The algorithm could be optimized with better data structures than simple lists if it becomes a performance bottleneck