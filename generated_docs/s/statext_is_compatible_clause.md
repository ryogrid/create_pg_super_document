# statext_is_compatible_clause

## Location
src/backend/statistics/extended_stats.c: 1558 - 1695

## Overview
Wrapper function that determines if a clause is compatible with MCV lists by handling RestrictInfo structures and performing security permission checks.

## Definition


## Detailed Description
This function serves as the public interface for clause compatibility checking with extended statistics MCV lists. It handles RestrictInfo superstructure that wraps actual clauses and performs essential security checks. The function first handles special cases like bare BoolExpr AND clauses, then validates that clauses reference only the target relation and are not pseudoconstants. It delegates the core compatibility analysis to statext_is_compatible_clause_internal and performs additional permission checks when non-leakproof operators are involved to ensure users cannot access data they lack permissions for through statistics inference.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and security information
- : Clause to be inspected (in RestrictInfo form)
- : Relation index that all variables in the clause must belong to
- : Input/output bitmap collecting attribute numbers of all mentioned variables
- : Input/output list collecting primitive subclauses within the clause tree

## Dependencies
- Functions called/Symbols referenced:
  - is_andclause
  - IsA (macro for type checking)
  - bms_get_singleton_member
  - statext_is_compatible_clause_internal
  - bms_next_member
  - bms_add_member
  - pull_varattnos
  - all_rows_selectable
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - statext_is_compatible_clause (recursive calls for AND clauses)
  - statext_mcv_clauselist_selectivity

## Notes and Other Information
The function implements important security measures by checking column-level permissions when non-leakproof operators are present, preventing information leakage through statistics. It handles the impedance mismatch between different attribute numbering schemes used internally. Special handling for AND clauses is necessary because the restrictinfo machinery doesn't create RestrictInfos for top-level AND operations. The permission checking is particularly important for inheritance hierarchies where parent table permissions don't guarantee child table column access.