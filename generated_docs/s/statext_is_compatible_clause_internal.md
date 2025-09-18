# statext_is_compatible_clause_internal

## Location
src/backend/statistics/extended_stats.c: 1331 - 1557

## Overview
Recursively determines if a clause is compatible with MCV (Most Common Values) lists by analyzing the clause structure and extracting supported sub-expressions and variables.

## Definition


## Detailed Description
This internal function recursively examines SQL clauses to determine compatibility with extended statistics MCV lists. It supports a specific set of clause types including OpExprs with comparison operators (=, <, >, >=, <=), NULL tests, ScalarArrayOpExprs (IN/ANY/ALL), and Boolean combinations (AND/OR/NOT). The function extracts variable attribute numbers and sub-expressions that need to be matched against statistics objects. It also tracks the leakproofness of operators to ensure security constraints are maintained during statistics-based estimation.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context information
- : Node representing the (sub)clause to be inspected (bare clause, not RestrictInfo)
- : Relation index that all variables in the clause must belong to
- : Input/output bitmap collecting attribute numbers of mentioned variables
- : Input/output list collecting primitive subclauses within the clause tree
- : Input/output flag tracking leakproofness of the clause tree (starts true, set false if non-leakproof operators found)

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - AttrNumberIsForUserDefinedAttr
  - bms_add_member
  - is_opclause
  - examine_opclause_args
  - get_oprrest
  - get_opcode
  - get_func_leakproof
  - is_andclause
  - is_orclause
  - is_notclause
  - lappend
- Called from (representative examples):
  - statext_is_compatible_clause_internal (recursive calls)
  - statext_is_compatible_clause

## Notes and Other Information
The function uses recursive descent parsing to handle nested clause structures. It rejects system attributes and whole-row variables since statistics cannot be collected on them. For operator expressions, it validates that operators use supported selectivity estimation functions (F_EQSEL, F_NEQSEL, etc.). The leakproof tracking ensures that security-sensitive queries maintain their security properties when using extended statistics. Future expansions may support more complex cases like Var op Var comparisons.