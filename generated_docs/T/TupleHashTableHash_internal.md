# TupleHashTableHash_internal

## Location
[src/backend/executor/execGrouping.c:424-493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execGrouping.c#L424-L493)

## Overview
Internal hash computation function that calculates hash values for tuples using column-wise hashing with rotation and bit perturbation, serving as the core hash algorithm for tuple hash tables.

## Definition
```c
static uint32 TupleHashTableHash_internal(struct tuplehash_hash *tb, const MinimalTuple tuple)
```

## Detailed Description
TupleHashTableHash_internal is the low-level hash computation engine for PostgreSQL's tuple hash tables. It implements a sophisticated hash algorithm that processes each key column of a tuple individually, combining their hash values using bit rotation and XOR operations to create a well-distributed final hash value.

The function supports two modes of operation:
1. **Input slot mode**: When tuple is NULL, it hashes the current input tuple from hashtable->inputslot using input hash functions
2. **Stored tuple mode**: When tuple is provided, it hashes a tuple already stored in the table (though this case is noted as never occurring in current usage)

The hash computation process involves:
- Starting with an initialization vector (hash_iv) 
- For each key column: rotating the current hash left by 1 bit, computing the column's hash, and XORing it with the accumulated hash
- Applying MurmurHash32 to the final combined value for optimal bit perturbation

## Parameters / Member Variables
- `tb`: Pointer to the tuplehash_hash structure containing hash table metadata
- `tuple`: MinimalTuple to hash, or NULL to use the input slot

## Dependencies
- Functions called/Symbols referenced:
  - ExecStoreMinimalTuple
  - pg_rotate_left32
  - slot_getattr
  - FunctionCall1Coll
  - DatumGetUInt32
  - murmurhash32
- Called from (representative examples):
  - LookupTupleHashEntry (execGrouping.c:319)
  - TupleHashTableHash (execGrouping.c:347)
  - SH_HASH_KEY macro (execGrouping.c:39)

## Notes and Other Information
- Uses bit rotation (pg_rotate_left32) to ensure good hash distribution across columns
- Treats NULL values as having hash key 0, maintaining consistent behavior for nullable columns
- Applies MurmurHash32 as a final step to achieve near-perfect bit perturbation
- The hash algorithm is designed to minimize collisions and ensure even distribution across hash buckets
- Memory context must be properly set by caller as the function doesn't change CurrentMemoryContext
- The stored tuple mode is documented as never occurring in current usage patterns
- Critical for performance of grouping, aggregation, and set operations in PostgreSQL's executor