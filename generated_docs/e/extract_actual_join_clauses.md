# extract_actual_join_clauses

## Location
src/backend/optimizer/util/restrictinfo.c: 522 - 583

## Overview
Separates join clauses into two categories: those that semantically belong to the current join level and those that were pushed down from higher levels, specifically designed for outer join processing.

## Definition
```c
void extract_actual_join_clauses(List *restrictinfo_list, Relids joinrelids, List **joinquals, List **otherquals)
```

## Detailed Description
This function performs sophisticated clause categorization for outer joins, where the distinction between native join clauses and pushed-down clauses is semantically important. Unlike inner joins where clause placement is primarily an optimization concern, outer joins require careful handling of where clauses are evaluated to maintain correct NULL-extension semantics.

The function uses the RINFO_IS_PUSHED_DOWN macro to determine whether each clause was pushed down from a higher join level. Native join clauses (those that semantically belong to the current join) are placed in the joinquals list, while pushed-down clauses go into otherquals. Both categories exclude pseudoconstant and constant-TRUE clauses to ensure only meaningful executable conditions are included.

## Parameters / Member Variables
- `restrictinfo_list`: Input list of RestrictInfo pointers to be categorized
- `joinrelids`: Set of relation IDs participating in the current join, used to determine push-down status
- `joinquals`: Output parameter - pointer to list that will receive native join qualification clauses
- `otherquals`: Output parameter - pointer to list that will receive pushed-down qualification clauses

## Dependencies
- Functions called/Symbols referenced:
  - RINFO_IS_PUSHED_DOWN macro (Line 536) - determines if clause was pushed down from higher level
  - rinfo_is_constant_true (Lines 539, 546) - filters out constant TRUE clauses
  - lfirst_node macro - for safe list iteration
  - lappend - to build result lists
  - NIL - PostgreSQL's empty list constant
- Called from (representative examples):
  - create_nestloop_plan (src/backend/optimizer/plan/createplan.c:4398)
  - create_mergejoin_plan (src/backend/optimizer/plan/createplan.c:4486)
  - create_hashjoin_plan (src/backend/optimizer/plan/createplan.c:4788)
  - make_simple_restrictinfo (src/include/optimizer/restrictinfo.h:42)

## Notes and Other Information
- This function is specifically designed for outer joins where clause placement affects query semantics, not just performance
- The assertion that joinquals shouldn't be pseudoconstant reflects the expectation that true join conditions are always variable expressions
- Pushed-down clauses may include pseudoconstants (hence the additional check), but native join clauses should not
- The function modifies the output parameters in-place, initializing them to NIL and building them incrementally
- Critical for maintaining SQL standard compliance in outer join processing where premature clause evaluation can change result correctness