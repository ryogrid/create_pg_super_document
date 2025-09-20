# statext_is_compatible_clause_internal

## Location
[src/backend/statistics/extended_stats.c:1331-1557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1331-L1557)

## Overview
Recursively determines if a clause is compatible with MCV (Most Common Values) lists by analyzing the clause structure and extracting supported sub-expressions and variables.

## Definition

```c
static bool
statext_is_compatible_clause_internal(PlannerInfo *root, Node *clause,
									  Index relid, Bitmapset **attnums,
									  List **exprs, bool *leakproof)
```
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
  - [bms_add_member](../b/bms_add_member.md)
  - [is_opclause](../i/is_opclause.md)
  - [examine_opclause_args](../e/examine_opclause_args.md)
  - [get_oprrest](../g/get_oprrest.md)
  - [get_opcode](../g/get_opcode.md)
  - [get_func_leakproof](../g/get_func_leakproof.md)
  - [is_andclause](../i/is_andclause.md)
  - [is_orclause](../i/is_orclause.md)
  - [is_notclause](../i/is_notclause.md)
  - lappend
- Called from (representative examples):
  - [statext_is_compatible_clause_internal](statext_is_compatible_clause_internal.md) (recursive calls)
  - [statext_is_compatible_clause](statext_is_compatible_clause.md)

## Notes and Other Information
The function uses recursive descent parsing to handle nested clause structures. It rejects system attributes and whole-row variables since statistics cannot be collected on them. For operator expressions, it validates that operators use supported selectivity estimation functions (F_EQSEL, F_NEQSEL, etc.). The leakproof tracking ensures that security-sensitive queries maintain their security properties when using extended statistics. Future expansions may support more complex cases like Var op Var comparisons.