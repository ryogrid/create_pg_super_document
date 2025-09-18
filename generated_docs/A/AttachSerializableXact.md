# AttachSerializableXact

## Location
src/backend/storage/lmgr/predicate.c: 5045 - 5053

## Overview
AttachSerializableXact allows parallel workers to import and attach to the leader's SERIALIZABLEXACT, enabling consistent serializable transaction behavior across parallel query participants.

## Definition
void AttachSerializableXact(SerializableXactHandle handle)

## Detailed Description
This function is the counterpart to ShareSerializableXact in PostgreSQL's parallel query support for serializable isolation. When a parallel worker process starts, it needs to attach to the same serializable transaction context as the leader to ensure consistent predicate locking and conflict detection. AttachSerializableXact takes a handle (obtained from ShareSerializableXact in the leader) and makes it the worker's active serializable transaction.

The function performs validation to ensure the worker doesn't already have an active serializable transaction, then sets up the shared context and initializes the local predicate lock hash table if needed. This ensures that the worker can participate in the same serialization conflict detection as the leader process.

## Parameters / Member Variables
- : A SerializableXactHandle obtained from ShareSerializableXact() in the leader process, representing the shared SERIALIZABLEXACT object

## Dependencies
- Functions called/Symbols referenced:
  - MySerializableXact (global variable)
  - InvalidSerializableXact (constant)
  - SERIALIZABLEXACT (type cast)
  - CreateLocalPredicateLockHash (function call)
- Called from (representative examples):
  - ParallelWorkerMain (in src/backend/access/transam/parallel.c:1538)

## Notes and Other Information
- The function includes an Assert to ensure MySerializableXact is InvalidSerializableXact, meaning the worker must not already have an active serializable transaction
- If the handle represents a valid serializable transaction (not InvalidSerializableXact), the function calls CreateLocalPredicateLockHash() to set up local predicate lock tracking
- This function is only called in parallel worker processes, never in the leader
- The handle is treated as an opaque pointer but is actually a pointer to a SERIALIZABLEXACT structure
- Essential for maintaining ACID properties and serializable isolation semantics in parallel queries
- Part of the broader parallel query infrastructure that ensures transaction consistency across multiple processes