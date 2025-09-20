# clause_is_strict_for

## Location
[src/backend/optimizer/util/predtest.c:1460-1661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1460-L1661)

## Overview
Determines if a clause returns NULL (or FALSE) when a specific subexpression yields NULL, implementing strictness analysis for PostgreSQL's predicate testing system.

## Definition

```c
struct it into an
	 * AND or OR tree, as for example if it has too many array elements.
	 */
	if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		Node	   *scalarnode = (Node *) linitial(saop->args);
		Node	   *arraynode = (Node *) lsecond(saop->args);

		/*
		 * If we can prove the scalar input to be null, and the operator is
		 * strict, then the SAOP result has to be null --- unless the array is
		 * empty.  For an empty array, we'd get either false (for ANY) or true
		 * (for ALL).  So if allow_false = true then the proof succeeds anyway
		 * for the ANY case; otherwise we can only make the proof if we can
		 * prove the array non-empty.
		 */
		if (clause_is_strict_for(scalarnode, subexpr, false) &&
			op_strict(saop->opno))
		{
			int			nelems = 0;

			if (allow_false && saop->useOr)
				return true;	/* can succeed even if array is empty */

			if (arraynode && IsA(arraynode, Const))
			{
				Const	   *arrayconst = (Const *) arraynode;
				ArrayType  *arrval;

				/*
				 * If array is constant NULL then we can succeed, as in the
				 * case below.
				 */
				if (arrayconst->constisnull)
					return true;

				/* Otherwise, we can compute the number of elements. */
				arrval = DatumGetArrayTypeP(arrayconst->constvalue);
				nelems = ArrayGetNItems(ARR_NDIM(arrval), ARR_DIMS(arrval));
			}
			else if (arraynode && IsA(arraynode, ArrayExpr) &&
					 !((ArrayExpr *) arraynode)->multidims)
			{
				/*
				 * We can also reliably count the number of array elements if
				 * the input is a non-multidim ARRAY[] expression.
				 */
				nelems = list_length(((ArrayExpr *) arraynode)->elements);
			}

			/* Proof succeeds if array is definitely non-empty */
			if (nelems > 0)
				return true;
		}

		/*
		 * If we can prove the array input to be null, the proof succeeds in
		 * all cases, since ScalarArrayOpExpr will always return NULL for a
		 * NULL array.  Otherwise, we're done here.
		 */
		return clause_is_strict_for(arraynode, subexpr, false);
	}

	/*
	 * When recursing into an expression, we might find a NULL constant.
	 * That's certainly NULL, whether it matches subexpr or not.
	 */
	if (IsA(clause, Const))
		return ((Const *) clause)->constisnull;
```
## Detailed Description
This function performs strictness analysis to prove whether a clause will definitely return NULL (or optionally FALSE) if a given subexpression evaluates to NULL. This is crucial for predicate testing logic where the optimizer needs to understand how NULL values propagate through expressions.

The function implements several layers of strictness detection:

1. **Direct equality**: If clause equals subexpr, it's trivially strict
2. **Strict operators/functions**: If the clause uses strict operators or functions, NULL inputs guarantee NULL outputs
3. **Type coercion strictness**: Various coercion operations (CoerceViaIO, ArrayCoerceExpr, ConvertRowtypeExpr, CoerceToDomain) preserve NULL values
4. **ScalarArrayOpExpr handling**: Special logic for array operations considering empty array edge cases
5. **NULL constants**: Direct NULL constants are always considered strict

The allow_false parameter provides flexibility for top-level boolean contexts where proving "not TRUE" is sufficient instead of proving strict NULL propagation.

## Parameters / Member Variables
- : The expression to analyze for strictness behavior
- : The subexpression that might be NULL, causing strictness
- : Whether proving FALSE result (not just NULL) is acceptable for top-level boolean expressions

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for node type checking)
  - [equal](../e/equal.md) (for expression equality)
  - [is_opclause](../i/is_opclause.md), op_strict (for operator strictness)
  - [is_funcclause](../i/is_funcclause.md), func_strict (for function strictness)
  - linitial, lsecond (for list access)
  - DatumGetArrayTypeP, ArrayGetNItems, ARR_NDIM, ARR_DIMS (for array analysis)
  - [clause_is_strict_for](clause_is_strict_for.md) (recursive calls)
- Called from (representative examples):
  - [predicate_implied_by_simple_clause](../p/predicate_implied_by_simple_clause.md)
  - [predicate_refuted_by_simple_clause](../p/predicate_refuted_by_simple_clause.md)
  - [clause_is_strict_for](clause_is_strict_for.md) (recursive calls)

## Notes and Other Information
- Handles RelabelType nodes transparently by looking through them to match underlying expressions
- Assumes at least one input expression is immutable (verified by caller)
- Uses recursive analysis with allow_false=false for internal subexpressions to ensure actual NULL propagation
- Special handling for ScalarArrayOpExpr considers empty array edge cases where ANY returns false and ALL returns true
- The function is self-recursive, building up strictness proofs through expression trees
- Returns false for safety if inputs are NULL or unrecognized expression types