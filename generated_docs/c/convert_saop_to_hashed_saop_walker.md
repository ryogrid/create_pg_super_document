# convert_saop_to_hashed_saop_walker

## Location
[src/backend/optimizer/util/clauses.c:2293-2394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L2293-L2394)

## Overview
This static function implements the recursive tree-walking logic to identify and optimize ScalarArrayOpExpr nodes by determining when hash table evaluation is beneficial and setting up the necessary hash function information.

## Definition

```c
static bool
convert_saop_to_hashed_saop_walker(Node *node, void *context)
```
## Detailed Description
The  function is the core implementation that performs the actual analysis and optimization of ScalarArrayOpExpr nodes. It recursively traverses expression trees and applies hash table optimization when specific conditions are met.

The function handles two main scenarios:
1. **OR operations (useOr = true)**: For expressions like "x IN (1,2,3)", it checks if both operands have compatible hash functions and sets hashfuncid when the array is large enough.
2. **NOT operations (useOr = false)**: For expressions like "x NOT IN (1,2,3)", it verifies that a valid negator operator exists with compatible hash functions, then sets both hashfuncid and negfuncid for the executor.

Key validation steps include:
- Ensuring the array argument is a constant and non-null
- Verifying hash function compatibility between left and right operands  
- Checking array size against MIN_ARRAY_SIZE_FOR_HASHED_SAOP threshold
- For NOT operations, validating the existence and hashability of the negator operator

## Parameters / Member Variables
- : Node pointer to the current node being examined in the expression tree
- : Void pointer for context data (currently unused, passed as NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [ScalarArrayOpExpr](../S/ScalarArrayOpExpr.md)
  - lsecond
  - [get_op_hash_functions](../g/get_op_hash_functions.md)
  - [ArrayGetNItems](../A/ArrayGetNItems.md), ARR_NDIM, ARR_DIMS
  - MIN_ARRAY_SIZE_FOR_HASHED_SAOP
  - [get_negator](../g/get_negator.md)
  - [get_opcode](../g/get_opcode.md)
  - expression_tree_walker
- Called from (representative examples):
  - [convert_saop_to_hashed_saop](convert_saop_to_hashed_saop.md) (clauses.c:2289)
  - (recursively calls itself via expression_tree_walker)

## Notes and Other Information
- Returns false when a ScalarArrayOpExpr is found and processed (to avoid recursing into its arguments)
- Uses expression_tree_walker for recursive traversal of non-ScalarArrayOpExpr nodes
- The MIN_ARRAY_SIZE_FOR_HASHED_SAOP threshold prevents hash table overhead for small arrays
- Critical for performance optimization of large IN/NOT IN clauses in SQL queries
- Sets up executor-specific fields (hashfuncid, negfuncid) that enable hash-based array evaluation

## Simplified Source

```c
static bool
convert_saop_to_hashed_saop_walker(Node *node, void *context)
{
    if (node == NULL)
        return false;

    if (IsA(node, ScalarArrayOpExpr))
    {
        ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) node;
        Expr *arrayarg = (Expr *) lsecond(saop->args);
        Oid lefthashfunc, righthashfunc;

        // Check if array argument is a non-null constant
        if (arrayarg && IsA(arrayarg, Const) &&
            !((Const *) arrayarg)->constisnull)
        {
            if (saop->useOr)  // Handle IN operations
            {
                // Check if operator has compatible hash functions
                if (get_op_hash_functions(saop->opno, &lefthashfunc, &righthashfunc) &&
                    lefthashfunc == righthashfunc)
                {
                    // Get array size to determine if hashing is worthwhile
                    ArrayType *arr = (ArrayType *) DatumGetPointer(((Const *) arrayarg)->constvalue);
                    int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

                    // Only use hash table for large arrays
                    if (nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP)
                        saop->hashfuncid = lefthashfunc;
                }
            }
            else  // Handle NOT IN operations
            {
                Oid negator = get_negator(saop->opno);

                // Check if negator operator is hashable
                if (OidIsValid(negator) &&
                    get_op_hash_functions(negator, &lefthashfunc, &righthashfunc) &&
                    lefthashfunc == righthashfunc)
                {
                    // Get array size and set up hash functions for NOT IN
                    ArrayType *arr = (ArrayType *) DatumGetPointer(((Const *) arrayarg)->constvalue);
                    int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));

                    if (nitems >= MIN_ARRAY_SIZE_FOR_HASHED_SAOP)
                    {
                        saop->hashfuncid = lefthashfunc;
                        saop->negfuncid = get_opcode(negator);  // For executor lookups
                    }
                }
            }
        }
        return false;  // Don't recurse into SAOP arguments
    }

    // Continue walking the expression tree
    return expression_tree_walker(node, convert_saop_to_hashed_saop_walker, NULL);
}
```