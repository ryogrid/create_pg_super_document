# ParallelApplyWorkerEntry

## Location
[src/backend/replication/logical/applyparallelworker.c:215-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L215-L219)

## Overview
ParallelApplyWorkerEntry is a hash table entry structure used to map transaction IDs (xid) to parallel apply worker state information in PostgreSQL's logical replication system.

## Definition

```c
typedef struct ParallelApplyWorkerEntry
{
	TransactionId xid;			/* Hash key -- must be first */
	ParallelApplyWorkerInfo *winfo;
} ParallelApplyWorkerEntry;
```
## Detailed Description
This structure serves as a hash table entry in PostgreSQL's parallel apply worker management system for logical replication. It creates a mapping between transaction IDs and their corresponding parallel worker information, enabling efficient lookup and management of worker processes handling specific transactions.

The structure is designed specifically for use with PostgreSQL's hash table implementation, where the first field (xid) acts as the hash key. This design allows the logical replication system to quickly locate which parallel worker is assigned to process changes for a particular transaction.

The hash table using these entries (ParallelApplyTxnHash) is created when the first parallel worker is allocated and serves as the central registry for tracking active parallel apply workers and their assigned transactions.

## Parameters / Member Variables
- : Transaction ID that serves as the hash key for the entry. Must be positioned first in the structure to comply with PostgreSQL's hash table requirements. This identifies the specific transaction being processed by the associated worker.
- : Pointer to ParallelApplyWorkerInfo structure containing detailed information about the parallel worker assigned to process this transaction, including communication queues, shared memory segments, and worker state.

## Dependencies
- Functions called/Symbols referenced:
  - ParallelApplyWorkerInfo
  - TransactionId
- Called from (representative examples):
  - pa_allocate_worker (creates new entries when allocating workers to transactions)
  - pa_find_worker (searches for existing entries to locate workers for specific transactions)

## Notes and Other Information
- The structure is specifically designed for hash table usage, with the hash key (xid) positioned as the first member as required by PostgreSQL's hash table implementation.
- Entries are created in the ApplyContext memory context and managed through the ParallelApplyTxnHash hash table.
- The hash table is lazily initialized on first worker allocation to avoid overhead when parallel apply is not used.
- This structure is part of PostgreSQL's logical replication parallel apply worker infrastructure, which allows multiple worker processes to apply changes from different transactions concurrently for improved performance.