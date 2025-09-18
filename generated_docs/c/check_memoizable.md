# check_memoizable

## Location
src/backend/optimizer/plan/initsplan.c: 3439 - 3473

## Overview
Determines if a restriction clause is suitable for use with a Memoize node and sets the hash equality operators needed for caching operations.

## Definition


## Detailed Description
This function evaluates whether a restriction clause can benefit from PostgreSQL's memoization optimization, which was introduced to improve the performance of nested loop joins by caching the results of expensive sub-plans. Memoize nodes act as a cache layer that can dramatically reduce redundant computation when the outer relation contains many duplicate values that would otherwise cause repeated execution of the same inner sub-plan.

The function performs several key operations:
1. Validates that the clause is not pseudoconstant and is a binary operator expression
2. Extracts the data types of both operands in the expression
3. Uses the type cache system to look up hash and equality operators for each operand type
4. Sets the left_hasheqoperator and right_hasheqoperator fields if appropriate hash and equality operators exist

The memoization system requires both hash and equality operators because it uses a hash table to cache results, where the hash function provides efficient lookup and the equality operator ensures correct matching of cached entries. This is particularly valuable for parameterized nested loop joins where the same parameter values may be encountered repeatedly.

## Parameters / Member Variables
- : RestrictInfo structure containing the clause to evaluate and the hash equality operator fields to populate for memoization

## Dependencies
- Functions called/Symbols referenced:
  - is_opclause (verifies expression is an operator clause)
  - exprType (determines expression data type)
  - lookup_type_cache (retrieves type cache information)
  - linitial/lsecond (list element access functions)
  - TYPECACHE_HASH_PROC/TYPECACHE_EQ_OPR (type cache flags)
  - OpExpr (operator expression node type)
  - TypeCacheEntry (structure containing type-related operators)
- Called from:
  - distribute_restrictinfo_to_rels (during restriction info distribution)
  - build_implied_join_equality (when building implied equality conditions)

## Notes and Other Information
- This is a static function within initsplan.c, serving as an internal optimization utility
- The function handles cases where both operands have the same type by reusing the type cache entry
- Memoize nodes are particularly effective for star-schema queries and correlated subqueries
- The hash equality operators identified here are used to build and probe the memoization cache
- Both hash_proc and eq_opr must be valid for the clause to be considered memoizable
- This optimization was introduced in PostgreSQL 14 as part of improving nested loop join performance
- The memoization cache has configurable size limits and eviction policies
- The function complements other join optimization techniques like hash and merge joins by providing a caching layer