# predicate_refuted_by

## Location
[src/backend/optimizer/util/predtest.c:222-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L222-L289)

## Overview
Tests whether a given predicate is logically refuted (proven false) by a set of clauses, supporting both strong and weak refutation semantics for constraint validation and query optimization.

## Definition
```c
bool predicate_refuted_by(List *predicate_list, List *clause_list, bool weak)
```

## Detailed Description
This function determines whether a predicate is logically refuted by another set of clauses. It supports two types of refutation:

- **Strong refutation**: Truth of clause_list implies falsity of predicate_list. Used to disprove CHECK constraints given WHERE clauses, proving that any row satisfying the WHERE clause would violate the CHECK constraint.
- **Weak refutation**: Truth of clause_list implies non-truth of predicate_list (i.e., predicate must yield false or NULL). Used to detect mutually contradictory WHERE clauses.

This is distinct from !(predicate_implied_by) though similar in technique. The function assumes flattened AND/OR structures and immutable functions for plan stability. Weak refutation can be proven in cases where strong refutation fails, making it useful for broader contradiction detection.

## Parameters / Member Variables
- `predicate_list`: List of clauses representing the predicate to be disproven (what we want to show is false)
- `clause_list`: List of clauses representing the known conditions (what we assume is true)
- `weak`: Boolean flag indicating whether to use weak refutation semantics (true) or strong refutation semantics (false)

## Dependencies
- Functions called/Symbols referenced:
  - [predicate_refuted_by_recurse](predicate_refuted_by_recurse.md)
  - [list_length](../l/list_length.md)
  - linitial
- Called from (representative examples):
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md) (constraint-based table exclusion)
  - [gen_partprune_steps_internal](../g/gen_partprune_steps_internal.md) (partition pruning)
  - [test_predtest](../t/test_predtest.md) (testing framework)

## Notes and Other Information
- Returns false for empty predicate lists (no predicate means no refutation possible)
- Returns false for empty clause lists when predicate is non-empty
- Optimizes single-element lists by unwrapping them to avoid unnecessary AND-recursion
- Primarily used in query optimization for constraint-based exclusion and partition pruning
- Strong refutation requires proving the predicate yields false, not just not-true
- Weak refutation is more permissive and useful for detecting contradictory conditions
- Does not currently support CHECK-vs-CHECK constraint refutation or WHERE-vs-CHECK refutation