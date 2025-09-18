# predicate_classify

## Location
[src/backend/optimizer/util/predtest.c:826-907](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L826-L907)

## Overview
Classifies expression nodes as AND-type, OR-type, or atomic expressions, and sets up iteration information for processing compound expressions in predicate testing.

## Definition
```c
static PredClass predicate_classify(Node *clause, PredIterInfo info)
```

## Detailed Description
This function analyzes an expression node to determine its logical structure and classify it into one of three categories:

- **CLASS_AND**: AND-type expressions including explicit AND clauses, implicit AND lists (such as RestrictInfo lists), and ScalarArrayOpExpr with useOr=false
- **CLASS_OR**: OR-type expressions including explicit OR clauses and ScalarArrayOpExpr with useOr=true  
- **CLASS_ATOM**: Atomic expressions that are neither AND nor OR compounds

For compound expressions (AND/OR types), the function populates the PredIterInfo structure with appropriate function pointers for iterating over the expressions components. It also enforces MAX_SAOP_ARRAY_SIZE limits on ScalarArrayOpExpr to prevent performance issues with very large arrays.

## Parameters / Member Variables
- `clause`: The expression node to classify (must not be NULL or RestrictInfo)
- `info`: Output parameter filled with iteration functions for compound expressions

## Dependencies
- Functions called/Symbols referenced:
  - [is_andclause](../i/is_andclause.md) (tests for AND boolean expressions)
  - [is_orclause](../i/is_orclause.md) (tests for OR boolean expressions)  
  - [list_startup_fn](../l/list_startup_fn.md), list_next_fn, list_cleanup_fn (iteration functions for lists)
  - [boolexpr_startup_fn](../b/boolexpr_startup_fn.md) (startup function for boolean expressions)
  - [arrayconst_startup_fn](../a/arrayconst_startup_fn.md), arrayconst_next_fn, arrayconst_cleanup_fn (iteration functions for constant arrays)
  - [arrayexpr_startup_fn](../a/arrayexpr_startup_fn.md), arrayexpr_next_fn, arrayexpr_cleanup_fn (iteration functions for array expressions)
  - DatumGetArrayTypeP, ArrayGetNItems, ARR_NDIM, ARR_DIMS (array processing functions)
  - lsecond (gets second list element)
  - MAX_SAOP_ARRAY_SIZE (size limit constant)
- Called from (representative examples):
  - [predicate_implied_by_recurse](predicate_implied_by_recurse.md) (for implication testing)
  - [predicate_refuted_by_recurse](predicate_refuted_by_recurse.md) (for refutation testing)

## Notes and Other Information
- Static function - internal implementation detail of predtest.c
- Returns PredClass enumeration value (CLASS_AND, CLASS_OR, or CLASS_ATOM)
- Lists are interpreted as implicit AND expressions (standard semantics for RestrictInfo lists)
- ScalarArrayOpExpr classification depends on the useOr flag and array size constraints
- Large ScalarArrayOpExpr arrays (> MAX_SAOP_ARRAY_SIZE) are treated as atoms to avoid performance issues
- Only handles non-null constant arrays and simple (non-multidimensional) ArrayExpr for decomposition
- Critical foundation function for all predicate implication and refutation logic
- Ensures that compound expressions can be systematically processed by the recursive testing functions