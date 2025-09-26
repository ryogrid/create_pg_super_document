# ExecHashGetHashValue

## Location
src/backend/executor/nodeHash.c: 1831 - 1938

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
- : HashJoinTable containing hash functions, collision information, and strictness flags for each hash key
- : ExprContext providing the execution context and tuple data for expression evaluation
- : List of ExprState nodes representing the hash key expressions to evaluate
- : Boolean indicating whether this is an outer tuple (HashJoin context) or inner tuple (Hash context)
- : Boolean controlling whether null attributes should be preserved (true) or cause rejection (false)
- : Output parameter receiving the computed 32-bit hash value

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - pg_rotate_left32
  - ExecEvalExpr
  - DatumGetUInt32
  - FunctionCall1Coll
- Called from (representative examples):
  - MultiExecPrivateHash
  - MultiExecParallelHash
  - ExecHashJoinOuterGetTuple
  - ExecParallelHashJoinOuterGetTuple
  - ExecParallelHashJoinPartitionOuter

## Notes and Other Information
- Returns false if a null attribute is encountered in strict mode without keep_nulls, indicating the tuple should be rejected
- Uses pg_rotate_left32 and XOR to combine hash values from multiple key expressions for better distribution
- Hash functions are selected based on whether processing inner or outer tuples
- Memory context management prevents memory leaks during repeated hash computations
- Supports both strict and non-strict join operators, though currently all hashjoinable operators are strict
- Null values in non-strict contexts contribute zero to the hash combination
- The hash computation respects collation settings for each hash key expression
- Expression context is reset before each computation to reclaim memory from previous evaluations