# select_equality_operator

## Location
src/backend/optimizer/path/equivclass.c: 1772 - 1807

## Overview
Selects a suitable equality operator for comparing two members of an EquivalenceClass, taking into account security requirements for leakproof operators.

## Definition


## Detailed Description
This function searches through the operator families associated with an EquivalenceClass to find an appropriate equality operator that can compare values of two specified data types. The function prioritizes security by ensuring that when barrier qualifications are present in the query (indicated by ec_max_security > 0), only leakproof operators are selected to prevent information leakage through operator behavior.

The function iterates through each operator family in the EquivalenceClass and attempts to find an equality operator using the B-tree equal strategy. If security constraints are active, it additionally verifies that the operator's underlying function is marked as leakproof before returning it.

## Parameters / Member Variables
- : Pointer to the EquivalenceClass containing operator families to search through
- : OID of the data type for the left operand
- : OID of the data type for the right operand

## Dependencies
- Functions called/Symbols referenced:
  - get_opfamily_member (to find operators within operator families)
  - get_opcode (to get the function implementing the operator)
  - get_func_leakproof (to check if the function is leakproof)
  - EquivalenceClass (struct type for equivalence class representation)
- Called from (representative examples):
  - generate_base_implied_equalities_const
  - generate_base_implied_equalities_no_const
  - generate_join_implied_equalities_normal
  - reconsider_outer_join_clause
  - reconsider_full_join_clause

## Notes and Other Information
- Returns InvalidOid if no suitable operator can be found for the given datatype combination
- The function implements a security-aware operator selection strategy where leakproof operators are required when barrier qualifications are present
- Uses BTEqualStrategyNumber to specifically look for equality operators within B-tree operator families
- This is a static function within equivclass.c, indicating it's an internal helper for equivalence class processing