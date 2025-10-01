# operator_predicate_proof

## Location
[src/backend/optimizer/util/predtest.c:1779-2031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1779-L2031)

## Overview
Performs predicate implication or refutation tests for "simple clause" predicates and restrictions when both are operator clauses using related operators and identical input expressions.

## Definition

```c
structs such as DistinctExpr.  But the planner isn't very smart
	 * about DistinctExpr in general, and this probably isn't the first place
	 * to fix if you want to improve that.
	 */
	if (!is_opclause(predicate))
		return false;
```
## Detailed Description
This function determines whether a predicate can be proven true or false based on a given clause constraint. It handles operator clauses with related operators (commutators, negators, or btree opfamily siblings) and identical input expressions. The function supports several proof patterns:

- Direct operator relationships (negators, commutators)
- Btree operator family relationships for same subexpressions
- Constant comparison proofs using btree semantics

The function assumes that related operators will not return one NULL and one non-NULL result for the same inputs, which simplifies the logic for strong vs. weak implications. For constant comparisons, it creates and executes a test expression to determine the relationship between the constants.

## Parameters / Member Variables
- : The predicate expression to be proven (must be an OpExpr)
- : The clause/constraint to use as evidence (must be an OpExpr) 
- : When false, attempts to prove predicate true; when true, attempts to prove predicate false
- : Indicates whether this is a weak implication/refutation test

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md)
  - [equal](../e/equal.md)
  - [operator_same_subexprs_proof](operator_same_subexprs_proof.md)
  - [get_commutator](../g/get_commutator.md)
  - [op_strict](op_strict.md)
  - [get_btree_test_op](../g/get_btree_test_op.md)
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [make_opclause](../m/make_opclause.md)
  - [fix_opfuncids](../f/fix_opfuncids.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md)
  - GetPerTupleExprContext
  - [FreeExecutorState](../F/FreeExecutorState.md)
- Called from:
  - [predicate_implied_by_simple_clause](../p/predicate_implied_by_simple_clause.md)
  - [predicate_refuted_by_simple_clause](../p/predicate_refuted_by_simple_clause.md)

## Notes and Other Information
The function requires both expressions to be binary operator clauses with matching collations. It handles various expression patterns by commuting operators when necessary to standardize the comparison. For constant comparisons, it creates an executor state to evaluate the test expression safely at plan time, assuming immutable operators.

## Simplified Source

```c
static bool
operator_predicate_proof(Expr *predicate, Node *clause, bool refute_it, bool weak)
{
    OpExpr *pred_opexpr, *clause_opexpr;
    Oid pred_op, clause_op, test_op;
    Node *pred_leftop, *pred_rightop, *clause_leftop, *clause_rightop;
    Const *pred_const, *clause_const;

    // Both must be binary operator clauses
    if (!is_opclause(predicate) || !is_opclause(clause))
        return false;

    pred_opexpr = (OpExpr *) predicate;
    clause_opexpr = (OpExpr *) clause;

    if (list_length(pred_opexpr->args) != 2 || list_length(clause_opexpr->args) != 2)
        return false;

    // Must have matching collations
    if (pred_opexpr->inputcollid != clause_opexpr->inputcollid)
        return false;

    pred_op = pred_opexpr->opno;
    clause_op = clause_opexpr->opno;

    // Extract operands
    pred_leftop = (Node *) linitial(pred_opexpr->args);
    pred_rightop = (Node *) lsecond(pred_opexpr->args);
    clause_leftop = (Node *) linitial(clause_opexpr->args);
    clause_rightop = (Node *) lsecond(clause_opexpr->args);

    // Try to match subexpressions in various patterns
    if (equal(pred_leftop, clause_leftop)) {
        if (equal(pred_rightop, clause_rightop)) {
            // Pattern: x op1 y and x op2 y
            return operator_same_subexprs_proof(pred_op, clause_op, refute_it);
        } else {
            // Both right operands must be constants
            if (!IsA(pred_rightop, Const) || !IsA(clause_rightop, Const))
                return false;
            pred_const = (Const *) pred_rightop;
            clause_const = (Const *) clause_rightop;
        }
    } else if (equal(pred_rightop, clause_rightop)) {
        // Both left operands must be constants, commute operators
        if (!IsA(pred_leftop, Const) || !IsA(clause_leftop, Const))
            return false;
        pred_const = (Const *) pred_leftop;
        clause_const = (Const *) clause_leftop;
        pred_op = get_commutator(pred_op);
        clause_op = get_commutator(clause_op);
        if (!OidIsValid(pred_op) || !OidIsValid(clause_op))
            return false;
    } else if (equal(pred_leftop, clause_rightop)) {
        if (equal(pred_rightop, clause_leftop)) {
            // Pattern: x op1 y and y op2 x
            pred_op = get_commutator(pred_op);
            if (!OidIsValid(pred_op))
                return false;
            return operator_same_subexprs_proof(pred_op, clause_op, refute_it);
        } else {
            // Mixed constant pattern, commute clause operator
            if (!IsA(pred_rightop, Const) || !IsA(clause_leftop, Const))
                return false;
            pred_const = (Const *) pred_rightop;
            clause_const = (Const *) clause_leftop;
            clause_op = get_commutator(clause_op);
            if (!OidIsValid(clause_op))
                return false;
        }
    } else if (equal(pred_rightop, clause_leftop)) {
        // Mixed constant pattern, commute predicate operator
        if (!IsA(pred_leftop, Const) || !IsA(clause_rightop, Const))
            return false;
        pred_const = (Const *) pred_leftop;
        clause_const = (Const *) clause_rightop;
        pred_op = get_commutator(pred_op);
        if (!OidIsValid(pred_op))
            return false;
    } else {
        return false;
    }

    // Handle NULL constants
    if (clause_const->constisnull) {
        if (!op_strict(clause_op))
            return false;
        if (!(weak && !refute_it))
            return true;
        if (pred_const->constisnull && op_strict(pred_op))
            return true;
        return false;
    }
    if (pred_const->constisnull) {
        if (weak && op_strict(pred_op))
            return true;
        return false;
    }

    // Get comparison operator for constants
    test_op = get_btree_test_op(pred_op, clause_op, refute_it);
    if (!OidIsValid(test_op))
        return false;

    // Create and execute test expression
    EState *estate = CreateExecutorState();
    MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    Expr *test_expr = make_opclause(test_op, BOOLOID, false,
                                   (Expr *) pred_const, (Expr *) clause_const,
                                   InvalidOid, pred_opexpr->inputcollid);
    fix_opfuncids((Node *) test_expr);
    ExprState *test_exprstate = ExecInitExpr(test_expr, NULL);

    bool isNull;
    Datum test_result = ExecEvalExprSwitchContext(test_exprstate,
                                                 GetPerTupleExprContext(estate),
                                                 &isNull);

    MemoryContextSwitchTo(oldcontext);
    FreeExecutorState(estate);

    if (isNull) {
        elog(DEBUG2, "null predicate test result");
        return false;
    }
    return DatumGetBool(test_result);
}
```