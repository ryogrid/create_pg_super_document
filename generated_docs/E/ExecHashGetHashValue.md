# ExecHashGetHashValue

## Location
[src/backend/executor/nodeHash.c:1831-1938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L1831-L1938)

## Overview
Computes hash values for tuples in hash joins by evaluating hash key expressions and combining them using rotation and XOR operations, with support for null handling and strict/non-strict join semantics.

## Definition

```c
bool
ExecHashGetHashValue(HashJoinTable hashtable,
					 ExprContext *econtext,
					 List *hashkeys,
					 bool outer_tuple,
					 bool keep_nulls,
					 uint32 *hashvalue)
```
## Detailed Description
This function computes hash values for tuples in both regular and parallel hash joins by evaluating a list of hash key expressions and combining their hash codes. It uses a rotate-left and XOR combination strategy to distribute hash values evenly across buckets. The function handles null values according to join semantics: for strict joins, null attributes cause immediate rejection unless keep_nulls is true, while for non-strict contexts, nulls are treated as having a hash code of zero.

The function operates in different contexts depending on the outer_tuple flag: for inner tuples (Hash node), it uses inner hash functions and expects Vars to reference the Hash node's child; for outer tuples (HashJoin node), it uses outer hash functions and expects Vars to have OUTER_VAR varno. Memory management is carefully handled with expression context resets to prevent memory leaks during hash computation.

## Parameters / Member Variables
- `hashtable`: HashJoinTable containing hash functions, collision information, and strictness flags for each hash key
- `*econtext`: ExprContext providing the execution context and tuple data for expression evaluation
- `*hashkeys`: List of ExprState nodes representing the hash key expressions to evaluate
- `outer_tuple`: Boolean indicating whether this is an outer tuple (HashJoin context) or inner tuple (Hash context)
- `keep_nulls`: Boolean controlling whether null attributes should be preserved (true) or cause rejection (false)
- `*hashvalue`: Output parameter receiving the computed 32-bit hash value
## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - [pg_rotate_left32](../p/pg_rotate_left32.md)
  - [ExecEvalExpr](ExecEvalExpr.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
- Called from (representative examples):
  - [MultiExecPrivateHash](../M/MultiExecPrivateHash.md)
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md)
  - [ExecHashJoinOuterGetTuple](ExecHashJoinOuterGetTuple.md)
  - [ExecParallelHashJoinOuterGetTuple](ExecParallelHashJoinOuterGetTuple.md)
  - [ExecParallelHashJoinPartitionOuter](ExecParallelHashJoinPartitionOuter.md)

## Notes and Other Information
- Returns false if a null attribute is encountered in strict mode without keep_nulls, indicating the tuple should be rejected
- Uses pg_rotate_left32 and XOR to combine hash values from multiple key expressions for better distribution
- [Hash](../H/Hash.md) functions are selected based on whether processing inner or outer tuples
- Memory context management prevents memory leaks during repeated hash computations
- Supports both strict and non-strict join operators, though currently all hashjoinable operators are strict
- Null values in non-strict contexts contribute zero to the hash combination
- The hash computation respects collation settings for each hash key expression
- Expression context is reset before each computation to reclaim memory from previous evaluations

## Simplified Source

```c
bool
ExecHashGetHashValue(HashJoinTable hashtable,
                     ExprContext *econtext,
                     List *hashkeys,
                     bool outer_tuple,
                     bool keep_nulls,
                     uint32 *hashvalue)
{
    uint32 hashkey = 0;
    FmgrInfo *hashfunctions;
    int i = 0;

    // Reset expression context to prevent memory leaks
    ResetExprContext(econtext);

    // Choose hash functions based on tuple source
    if (outer_tuple)
        hashfunctions = hashtable->outer_hashfunctions;
    else
        hashfunctions = hashtable->inner_hashfunctions;

    // Process each hash key expression
    foreach(hk, hashkeys)
    {
        ExprState *keyexpr = (ExprState *) lfirst(hk);
        Datum keyval;
        bool isNull;

        // Rotate previous hash value and get next key value
        hashkey = pg_rotate_left32(hashkey, 1);
        keyval = ExecEvalExpr(keyexpr, econtext, &isNull);

        // Handle null values according to join semantics
        if (isNull)
        {
            if (hashtable->hashStrict[i] && !keep_nulls)
                return false; // Reject tuple for strict joins
            // For non-strict, leave hashkey unchanged (null = 0 hash)
        }
        else
        {
            // Compute hash value and combine with XOR
            uint32 hkey = DatumGetUInt32(FunctionCall1Coll(&hashfunctions[i],
                                                           hashtable->collations[i],
                                                           keyval));
            hashkey ^= hkey;
        }
        i++;
    }

    *hashvalue = hashkey;
    return true;
}
```

This simplified version shows the core hash computation algorithm: evaluate each hash key expression, handle nulls according to join strictness, and combine hash values using rotation and XOR for even distribution.