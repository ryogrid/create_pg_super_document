# ShareSerializableXact

## Location
[src/backend/storage/lmgr/predicate.c:5036-5044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L5036-L5044)

## Overview
ShareSerializableXact prepares the current SERIALIZABLEXACT for sharing with parallel workers, returning a handle that can be used by parallel workers to attach to the same serializable transaction context.

## Definition
SerializableXactHandle ShareSerializableXact(void)

## Detailed Description
This function is part of PostgreSQL's parallel query support for serializable isolation level transactions. When a parallel query is executed under serializable isolation, the leader process needs to share its serializable transaction context with parallel worker processes to ensure consistent predicate locking and serialization conflict detection across all participants. ShareSerializableXact simply returns the current process's MySerializableXact as an opaque handle that can be passed to worker processes.

The function is designed to be lightweight and safe - it performs no validation or state changes, merely providing access to the current serializable transaction object for sharing purposes.

## Parameters / Member Variables
(This function takes no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - MySerializableXact (global variable reference)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (in src/backend/access/transam/parallel.c:353)

## Notes and Other Information
- This function is only meaningful when called from a process that has an active serializable transaction (MySerializableXact != InvalidSerializableXact)
- The returned handle is intended to be passed to AttachSerializableXact() in parallel worker processes
- The handle is essentially a pointer to the SERIALIZABLEXACT structure, but is typed as void* for abstraction
- Part of PostgreSQL's parallel query infrastructure introduced to support serializable isolation in parallel queries