# operator_same_subexprs_proof

## Location
[src/backend/optimizer/util/predtest.c:2032-2073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L2032-L2073)

## Overview
Attempts to prove or refute a predicate assuming a clause with the same subexpressions is true, using operator relationships like negators or btree opfamily semantics.

## Definition

```c
typedef struct OprProofCacheKey
{
	Oid			pred_op;		/* predicate operator */
	Oid			clause_op;		/* clause operator */
} OprProofCacheKey;
```
## Detailed Description
This function handles the case where we have identical subexpressions in both the predicate and clause (e.g., EXPR1 clause_op EXPR2 vs EXPR1 pred_op EXPR2). It first applies simple logical rules: the predicate is proven true if the operators are identical, or proven false if they are negators of each other. If these simple rules don't apply, it delegates to operator_same_subexprs_lookup to check for relationships through btree operator families.

The function assumes immutability of the pred_op and relies on the fact that commutators and negators of immutable operators are also immutable.

## Parameters / Member Variables
- : OID of the predicate operator to be proven/refuted
- : OID of the clause operator assumed to be true
- : When false, attempts to prove predicate true; when true, attempts to prove predicate false

## Dependencies
- Functions called/Symbols referenced:
  - [get_negator](../g/get_negator.md)
  - [operator_same_subexprs_lookup](operator_same_subexprs_lookup.md)
- Called from:
  - [operator_predicate_proof](operator_predicate_proof.md) (twice - for direct and commuted cases)

## Notes and Other Information
This function handles the straightforward cases of operator relationships before falling back to more complex btree opfamily analysis. The "same operator" case typically won't reach this function when called directly, but can occur after operator commutation in the calling function.