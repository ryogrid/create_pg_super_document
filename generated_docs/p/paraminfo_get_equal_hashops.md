# paraminfo_get_equal_hashops

## Location
src/backend/optimizer/path/joinpath.c: 439 - 580

## Overview
Analyzes parameterization information and lateral variables to determine if they can be hashed for memoization, collecting the necessary expressions and operators for hash-based caching.

## Definition


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
  - clause_sides_match_join
  - contain_volatile_functions
  - lookup_type_cache
  - list_free
  - list_member
  - lappend_oid
  - linitial
  - lsecond
  - exprType
  - OidIsValid
  - TYPECACHE_HASH_PROC
  - TYPECACHE_EQ_OPR
- Called from (representative examples):
  - get_memoize_path

## Notes and Other Information
This function is static and used internally within joinpath.c as part of the memoization path generation logic. Binary mode is required when the function cannot guarantee that hash-based equality checking will be sufficient for correctness.

The function handles edge cases like duplicate parameter expressions by avoiding redundant entries while still ensuring binary mode is set when necessary. It also includes comprehensive memory management by freeing allocated lists when rejecting memoization.

The binary mode requirement is particularly important for floating-point comparisons where hash equality might not distinguish values that other operators can differentiate (like -0.0 vs +0.0).