# statext_is_compatible_clause

## Location
[src/backend/statistics/extended_stats.c:1558-1695](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L1558-L1695)

## Overview
Wrapper function that determines if a clause is compatible with MCV lists by handling RestrictInfo structures and performing security permission checks.

## Definition

```c
static bool
statext_is_compatible_clause(PlannerInfo *root, Node *clause, Index relid,
							 Bitmapset **attnums, List **exprs)
```
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
  - [is_andclause](../i/is_andclause.md)
  - IsA (macro for type checking)
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [statext_is_compatible_clause_internal](statext_is_compatible_clause_internal.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [pull_varattnos](../p/pull_varattnos.md)
  - all_rows_selectable
  - FirstLowInvalidHeapAttributeNumber
- Called from (representative examples):
  - [statext_is_compatible_clause](statext_is_compatible_clause.md) (recursive calls for AND clauses)
  - [statext_mcv_clauselist_selectivity](statext_mcv_clauselist_selectivity.md)

## Notes and Other Information
The function implements important security measures by checking column-level permissions when non-leakproof operators are present, preventing information leakage through statistics. It handles the impedance mismatch between different attribute numbering schemes used internally. Special handling for AND clauses is necessary because the restrictinfo machinery doesn't create RestrictInfos for top-level AND operations. The permission checking is particularly important for inheritance hierarchies where parent table permissions don't guarantee child table column access.