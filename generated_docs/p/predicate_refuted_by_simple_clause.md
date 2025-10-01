# predicate_refuted_by_simple_clause

## Location
[src/backend/optimizer/util/predtest.c:1225-1385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1225-L1385)

## Overview
Tests whether a simple clause predicate is refuted by another simple clause restriction, primarily supporting IS NULL/IS NOT NULL partition-defining constraints in PostgreSQL's query optimization.

## Definition

```c
static bool
predicate_refuted_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak)
```
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

## Simplified Source

```c
static bool
predicate_refuted_by_simple_clause(Expr *predicate, Node *clause, bool weak)
{
    // Allow interrupting long proof attempts
    CHECK_FOR_INTERRUPTS();

    // Same clause can't refute itself
    if ((Node *) predicate == clause)
        return false;

    // Handle clause-type-specific strategies
    switch (nodeTag(clause))
    {
        case T_NullTest:
            {
                NullTest *clausentest = (NullTest *) clause;

                // Skip row-level null tests
                if (clausentest->argisrow)
                    return false;

                switch (clausentest->nulltesttype)
                {
                    case IS_NULL:
                        {
                            // Check if predicate is IS NOT NULL for same arg
                            if (IsA(predicate, NullTest))
                            {
                                NullTest *predntest = (NullTest *) predicate;
                                if (!predntest->argisrow &&
                                    predntest->nulltesttype == IS_NOT_NULL &&
                                    equal(predntest->arg, clausentest->arg))
                                    return true;
                            }

                            // Weak refutation: foo IS NULL refutes strict predicates
                            if (weak &&
                                clause_is_strict_for((Node *) predicate,
                                                   (Node *) clausentest->arg, true))
                                return true;

                            return false;
                        }
                        break;
                    case IS_NOT_NULL:
                        break;
                }
            }
            break;
        default:
            break;
    }

    // Handle predicate-type-specific strategies
    switch (nodeTag(predicate))
    {
        case T_NullTest:
            {
                NullTest *predntest = (NullTest *) predicate;

                // Skip row-level null tests
                if (predntest->argisrow)
                    return false;

                switch (predntest->nulltesttype)
                {
                    case IS_NULL:
                        {
                            // Check if clause is IS NOT NULL for same arg
                            if (IsA(clause, NullTest))
                            {
                                NullTest *clausentest = (NullTest *) clause;
                                if (!clausentest->argisrow &&
                                    clausentest->nulltesttype == IS_NOT_NULL &&
                                    equal(clausentest->arg, predntest->arg))
                                    return true;
                            }

                            // foo IS NULL is refuted by strict clauses
                            if (clause_is_strict_for(clause,
                                                   (Node *) predntest->arg, true))
                                return true;
                        }
                        break;
                    case IS_NOT_NULL:
                        break;
                }

                return false;
            }
            break;
        default:
            break;
    }

    // Try operator-based proof for binary expressions
    return operator_predicate_proof(predicate, clause, true, weak);
}
```