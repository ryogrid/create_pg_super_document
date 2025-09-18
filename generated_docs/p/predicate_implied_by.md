# predicate_implied_by

## Location
[src/backend/optimizer/util/predtest.c:152-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L152-L221)

## Overview
Tests whether a given predicate is logically implied by a set of clauses, supporting both strong and weak implication semantics for query optimization and constraint validation.

## Definition
```c
bool predicate_implied_by(List *predicate_list, List *clause_list, bool weak)
```

## Detailed Description
This function determines whether a predicate (represented as a list of clauses) is logically implied by another set of clauses. It supports two types of implication:

- **Strong implication**: Truth of clause_list implies truth of predicate_list. Used to prove that rows satisfying one WHERE clause or index predicate must satisfy another.
- **Weak implication**: Non-falsity of clause_list implies non-falsity of predicate_list (where "non-false" means "either true or NULL"). Used to prove that rows satisfying one CHECK constraint must satisfy another.

The function assumes that both input lists represent AND-ed conditions at the top level, and that eval_const_expressions() has been applied to flatten nested AND/OR structures. It also requires that the predicate contains only immutable functions and operators to ensure plan stability.

## Parameters / Member Variables
- `predicate_list`: List of clauses representing the predicate to be proven (what we want to show is true)
- `clause_list`: List of clauses representing the known conditions (what we assume is true)  
- `weak`: Boolean flag indicating whether to use weak implication semantics (true) or strong implication semantics (false)

## Dependencies
- Functions called/Symbols referenced:
  - [predicate_implied_by_recurse](predicate_implied_by_recurse.md)
  - list_length
  - linitial
- Called from (representative examples):
  - [ConstraintImpliedByRelConstraint](../C/ConstraintImpliedByRelConstraint.md) (table constraint validation)
  - [build_paths_for_OR](../b/build_paths_for_OR.md) (query path planning)
  - [choose_bitmap_and](../c/choose_bitmap_and.md) (bitmap index planning)
  - [check_index_predicates](../c/check_index_predicates.md) (index predicate validation)
  - [create_indexscan_plan](../c/create_indexscan_plan.md) (index scan planning)

## Notes and Other Information
- Returns true for empty predicate lists (vacuous implication)
- Returns false for empty clause lists when predicate is non-empty
- Optimizes single-element lists by unwrapping them to avoid unnecessary AND-recursion
- Relies on immutability assumptions for correctness across plan creation and execution
- Used extensively in query optimization for index selection and constraint checking
- Strong implication can prove WHERE clause implies CHECK constraint, but may miss some valid cases