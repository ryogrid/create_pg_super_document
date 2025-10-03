# predicate_implied_by_simple_clause

## Location
[src/backend/optimizer/util/predtest.c:1098-1224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1098-L1224)

## Overview
Tests whether a simple clause predicate is implied by another simple clause restriction, used in PostgreSQL's predicate testing system for query optimization.

## Definition

```c
static bool
predicate_implied_by_simple_clause(Expr *predicate, Node *clause,
								   bool weak)
```
## Detailed Description
This function performs implication testing between two "simple clause" expressions to determine if the truth of one clause logically implies the truth of another predicate. It's a core component of PostgreSQL's predicate testing infrastructure used by the query optimizer to prove relationships between query conditions and index conditions.

The function implements several implication rules:
1. **Equality rule**: Any clause implies itself (reflexivity)
2. **Boolean equality handling**: Recognizes that "x = TRUE" is equivalent to "x" and "x = FALSE" is equivalent to "NOT x"
3. **NULL test implications**: For "foo IS NOT NULL" predicates under strong implication, checks if the clause is strict for the variable (would be false/NULL when the variable is NULL)
4. **Operator-based proofs**: Delegates to operator_predicate_proof() for binary operator expressions

The function supports both "weak" and "strong" implication modes, where strong implication has stricter requirements but can prove more cases.

## Parameters / Member Variables
- `*predicate`: The predicate expression that we want to prove is implied
- `*clause`: The restriction clause that potentially implies the predicate
- `weak`: Boolean flag indicating whether to use weak (true) or strong (false) implication semantics
## Dependencies
- Functions called/Symbols referenced:
  - [equal](../e/equal.md) (for expression equality testing)
  - nodeTag (for node type checking)
  - lsecond (for accessing second list element)
  - [is_notclause](../i/is_notclause.md) (for NOT clause detection)
  - [get_notclausearg](../g/get_notclausearg.md) (for extracting NOT clause argument)
  - [clause_is_strict_for](../c/clause_is_strict_for.md) (for strictness testing)
  - [operator_predicate_proof](../o/operator_predicate_proof.md) (for operator-based implication proofs)
- Called from (representative examples):
  - iterate_end
  - [predicate_implied_by_recurse](predicate_implied_by_recurse.md)

## Notes and Other Information
- Includes CHECK_FOR_INTERRUPTS() to allow interruption of long proof attempts
- Handles special cases for boolean equality operators (BooleanEqualOperator)
- Only processes IS_NOT_NULL null tests for implication (IS_NULL tests are handled elsewhere)
- The argisrow check ensures row-level null tests are excluded from simple processing
- Function assumes that expressions contain only immutable functions, which should be verified by the caller

## Simplified Source

```c
static bool predicate_implied_by_simple_clause(Expr *predicate, Node *clause, bool weak) {
    // Allow interrupting long proof attempts
    CHECK_FOR_INTERRUPTS();

    // Basic rule: any clause implies itself
    if (equal((Node *) predicate, clause))
        return true;

    // Handle specific clause types
    switch (nodeTag(clause)) {
        case T_OpExpr: {
            OpExpr *op = (OpExpr *) clause;

            // Handle boolean equality: "x = TRUE" implies "x", "x = FALSE" implies "NOT x"
            if (op->opno == BooleanEqualOperator) {
                Assert(list_length(op->args) == 2);
                Node *right_operand = lsecond(op->args);

                if (right_operand && IsA(right_operand, Const) && !((Const *) right_operand)->constisnull) {
                    Node *left_operand = linitial(op->args);

                    if (DatumGetBool(((Const *) right_operand)->constvalue)) {
                        // "X = true" implies "X"
                        if (equal(predicate, left_operand))
                            return true;
                    } else {
                        // "X = false" implies "NOT X"
                        if (is_notclause(predicate) && equal(get_notclausearg(predicate), left_operand))
                            return true;
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    // Handle specific predicate types
    switch (nodeTag(predicate)) {
        case T_NullTest: {
            NullTest *null_test = (NullTest *) predicate;

            if (null_test->nulltesttype == IS_NOT_NULL) {
                // For "foo IS NOT NULL": if clause is strict for foo,
                // then clause being true means foo cannot be NULL
                if (!weak && !null_test->argisrow &&
                    clause_is_strict_for(clause, (Node *) null_test->arg, true))
                    return true;
            }
            break;
        }
        default:
            break;
    }

    // Try operator-based proof rules for binary operators
    return operator_predicate_proof(predicate, clause, false, weak);
}
```