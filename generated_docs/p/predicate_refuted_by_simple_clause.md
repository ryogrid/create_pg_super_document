# predicate_refuted_by_simple_clause

## Location
src/backend/optimizer/util/predtest.c: 1225 - 1385

## Overview
Tests whether a simple clause predicate is refuted by another simple clause restriction, primarily supporting IS NULL/IS NOT NULL partition-defining constraints in PostgreSQL's query optimization.

## Definition


## Detailed Description
This function performs refutation testing between two "simple clause" expressions to determine if the truth of one clause logically contradicts (refutes) another predicate. It's designed as the complement to predicate_implied_by_simple_clause, focusing on proving when predicates cannot both be true simultaneously.

The function implements several refutation rules:
1. **Pointer equality check**: Quickly eliminates cases where predicate and clause are the same object (cannot refute itself)
2. **NULL test refutations**: Handles IS NULL vs IS NOT NULL contradictions for the same variable
3. **Strictness-based refutation**: Uses strictness properties where NULL values in variables make strict predicates false/NULL
4. **Operator-based proofs**: Delegates to operator_predicate_proof() for binary operator expressions

The main motivation is supporting partition pruning with IS NULL/IS NOT NULL constraints, where proving that a condition refutes a partition constraint allows the optimizer to exclude that partition.

## Parameters / Member Variables
- : The predicate expression that we want to prove is refuted
- : The restriction clause that potentially refutes the predicate
- : Boolean flag indicating whether to use weak (true) or strong (false) refutation semantics

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (for node type checking)
  - [equal](../e/equal.md) (for expression equality testing)
  - [clause_is_strict_for](../c/clause_is_strict_for.md) (for strictness testing)
  - [operator_predicate_proof](../o/operator_predicate_proof.md) (for operator-based refutation proofs)
- Called from (representative examples):
  - iterate_end
  - [predicate_refuted_by_recurse](predicate_refuted_by_recurse.md)

## Notes and Other Information
- Includes CHECK_FOR_INTERRUPTS() to allow interruption of long proof attempts
- Unlike implication testing, equal() clause checking is not useful since a clause cannot refute itself
- Row-level null tests (argisrow) are excluded from processing as they don't follow simple refutation rules
- The function handles both directions of NULL test refutation (IS NULL refutes IS NOT NULL and vice versa)
- Weak refutation allows broader proof cases using strictness properties
- Early returns are used when NULL test processing determines the final result, avoiding unnecessary operator proof attempts