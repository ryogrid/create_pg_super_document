# paraminfo_get_equal_hashops

## Location
[src/backend/optimizer/path/joinpath.c:439-580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L439-L580)

## Overview
Analyzes parameterization information and lateral variables to determine if they can be hashed for memoization, collecting the necessary expressions and operators for hash-based caching.

## Definition

```c
static bool
paraminfo_get_equal_hashops(PlannerInfo *root, ParamPathInfo *param_info,
							RelOptInfo *outerrel, RelOptInfo *innerrel,
							List **param_exprs, List **operators,
							bool *binary_mode)
```
## Detailed Description
This function examines the join clauses in a ParamPathInfo structure and the lateral variables of the inner relation to determine whether they can be used for hash-based memoization. It validates that all parameter expressions have appropriate hash functions and equality operators needed for efficient caching.

The function performs several key validations:
1. Checks that join clauses are compatible OpExpr nodes with exactly 2 arguments
2. Verifies that clauses match the expected join pattern (outer/inner relation sides)
3. Ensures hash equality operators exist for all parameter types
4. Validates that lateral variables have both hash procedures and equality operators
5. Handles volatile functions by rejecting them for caching

When successful, the function returns parallel lists of parameter expressions and their corresponding equality operators. It also determines whether binary mode is required, which happens when the join operator differs from the hash equality operator or when lateral variables are involved.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer context
- : ParamPathInfo containing the parameterization clauses to analyze
- : RelOptInfo for the outer relation in the join
- : RelOptInfo for the inner relation containing potential lateral variables
- : Output parameter receiving the list of hashable parameter expressions
- : Output parameter receiving the list of corresponding equality operators
- : Output parameter indicating whether strict binary comparison is required

## Dependencies
- Functions called/Symbols referenced:
  - [clause_sides_match_join](../c/clause_sides_match_join.md)
  - [contain_volatile_functions](../c/contain_volatile_functions.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [list_free](../l/list_free.md)
  - [list_member](../l/list_member.md)
  - [lappend_oid](../l/lappend_oid.md)
  - linitial
  - lsecond
  - [exprType](../e/exprType.md)
  - OidIsValid
  - TYPECACHE_HASH_PROC
  - TYPECACHE_EQ_OPR
- Called from (representative examples):
  - [get_memoize_path](../g/get_memoize_path.md)

## Notes and Other Information
This function is static and used internally within joinpath.c as part of the memoization path generation logic. Binary mode is required when the function cannot guarantee that hash-based equality checking will be sufficient for correctness.

The function handles edge cases like duplicate parameter expressions by avoiding redundant entries while still ensuring binary mode is set when necessary. It also includes comprehensive memory management by freeing allocated lists when rejecting memoization.

The binary mode requirement is particularly important for floating-point comparisons where hash equality might not distinguish values that other operators can differentiate (like -0.0 vs +0.0).

## Simplified Source

```c
static bool
paraminfo_get_equal_hashops(PlannerInfo *root, ParamPathInfo *param_info,
                            RelOptInfo *outerrel, RelOptInfo *innerrel,
                            List **param_exprs, List **operators,
                            bool *binary_mode)
{
    ListCell *lc;

    // Initialize output parameters
    *param_exprs = NIL;
    *operators = NIL;
    *binary_mode = false;

    // Process join clauses from param_info
    if (param_info != NULL)
    {
        foreach(lc, param_info->ppi_clauses)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            OpExpr *opexpr = (OpExpr *) rinfo->clause;
            Node *expr;
            Oid hasheqoperator;

            // Validate clause compatibility: must be 2-arg OpExpr matching join pattern
            if (!IsA(opexpr, OpExpr) || list_length(opexpr->args) != 2 ||
                !clause_sides_match_join(rinfo, outerrel, innerrel))
            {
                list_free(*operators);
                list_free(*param_exprs);
                return false;
            }

            // Extract outer expression and hash equality operator
            if (rinfo->outer_is_left)
            {
                expr = (Node *) linitial(opexpr->args);
                hasheqoperator = rinfo->left_hasheqoperator;
            }
            else
            {
                expr = (Node *) lsecond(opexpr->args);
                hasheqoperator = rinfo->right_hasheqoperator;
            }

            // Must have valid hash equality operator for memoization
            if (!OidIsValid(hasheqoperator))
            {
                list_free(*operators);
                list_free(*param_exprs);
                return false;
            }

            // Add unique expressions to parameter list
            if (!list_member(*param_exprs, expr))
            {
                *operators = lappend_oid(*operators, hasheqoperator);
                *param_exprs = lappend(*param_exprs, expr);
            }

            // Use binary mode when join operator differs from hash operator
            if (!OidIsValid(rinfo->hashjoinoperator))
                *binary_mode = true;
        }
    }

    // Process lateral variables for cache key
    foreach(lc, innerrel->lateral_vars)
    {
        Node *expr = (Node *) lfirst(lc);
        TypeCacheEntry *typentry;

        // Reject volatile functions (results can't be cached safely)
        if (contain_volatile_functions(expr))
        {
            list_free(*operators);
            list_free(*param_exprs);
            return false;
        }

        // Get hash and equality operators for this type
        typentry = lookup_type_cache(exprType(expr),
                                     TYPECACHE_HASH_PROC | TYPECACHE_EQ_OPR);

        // Must have both hash procedure and equality operator
        if (!OidIsValid(typentry->hash_proc) || !OidIsValid(typentry->eq_opr))
        {
            list_free(*operators);
            list_free(*param_exprs);
            return false;
        }

        // Add unique lateral vars to parameter list
        if (!list_member(*param_exprs, expr))
        {
            *operators = lappend_oid(*operators, typentry->eq_opr);
            *param_exprs = lappend(*param_exprs, expr);
        }

        // Lateral vars require binary mode due to unknown usage patterns
        *binary_mode = true;
    }

    return true;  // Memoization is possible
}
```