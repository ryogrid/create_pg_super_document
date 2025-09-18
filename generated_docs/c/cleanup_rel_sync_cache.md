# cleanup_rel_sync_cache

## Location
[src/backend/replication/pgoutput/pgoutput.c:2294-2328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L2294-L2328)

## Overview
Cleans up the relation sync cache by removing completed transaction IDs and updating schema state flags when streamed transactions commit or abort.

## Definition


## Detailed Description
This function performs cleanup operations on the relation synchronization cache when streamed transactions complete. It iterates through all entries in the RelationSyncCache hash table and performs the following operations:

1. **Transaction Cleanup**: Removes the specified transaction ID from each entry's streamed_txns list
2. **Schema State Management**: For committed transactions, sets the schema_sent flag to true, indicating the subscriber has received the schema information
3. **Memory Management**: Uses foreach_delete_current to safely remove transaction IDs from the list during iteration

The function handles both commit and abort scenarios differently - committed transactions result in schema_sent being set to true, while aborted transactions simply have their transaction IDs removed without state changes.

## Parameters / Member Variables
- : The transaction ID that has completed (committed or aborted)
- : Boolean flag indicating whether the transaction committed (true) or aborted (false)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table iteration)
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through hash entries)
  - foreach_xid (iterate through transaction ID list)
  - foreach_delete_current (safely remove current element during iteration)
- Called from (representative examples):
  - [pgoutput_stream_abort](../p/pgoutput_stream_abort.md) (when streamed transaction aborts)
  - [pgoutput_stream_commit](../p/pgoutput_stream_commit.md) (when streamed transaction commits)

## Notes and Other Information
- The function operates on the global RelationSyncCache hash table
- Uses PostgreSQL's hash table sequential scan macros for safe iteration
- The schema_sent flag optimization prevents unnecessary schema retransmission
- Transaction ID cleanup prevents memory leaks in long-running replication slots
- The foreach_delete_current macro ensures safe list modification during iteration
- Only affects entries that actually contain the specified transaction ID in their streamed_txns list