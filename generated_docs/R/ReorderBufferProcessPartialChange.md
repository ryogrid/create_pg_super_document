# ReorderBufferProcessPartialChange

## Location
[src/backend/replication/logical/reorderbuffer.c:737-805](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L737-L805)

## Overview
Manages partial changes during streaming of in-progress transactions, tracking incomplete changes like TOAST inserts and speculative inserts to ensure only complete changes are streamed.

## Definition


## Detailed Description
This function handles the tracking and management of partial changes during logical replication streaming. It identifies when transactions contain incomplete changes (such as TOAST table inserts or speculative inserts) and marks them appropriately to prevent streaming until the changes are complete. The function also triggers immediate streaming of previously serialized transactions once their partial changes become complete, reducing apply lag. It operates only when streaming is enabled and maintains transaction state flags to track partial change status.

## Parameters / Member Variables
- : The ReorderBuffer managing the streaming configuration and transaction state
- : The transaction containing the change (may be a subtransaction)
- : The specific change being processed that may be partial
- : Boolean flag indicating if this is a TOAST table insert operation

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferCanStream](ReorderBufferCanStream.md) (checks if streaming is enabled)
  - rbtxn_get_toptxn (gets the top-level transaction)
  - IsInsertOrUpdate (checks if change is insert or update)
  - IsSpecInsert (checks if change is speculative insert)
  - IsSpecConfirmOrAbort (checks if change confirms/aborts speculation)
  - [ReorderBufferCanStartStreaming](ReorderBufferCanStartStreaming.md) (checks if streaming can start)
  - [ReorderBufferStreamTXN](ReorderBufferStreamTXN.md) (initiates transaction streaming)
  - rbtxn_has_partial_change (checks partial change flag)
  - rbtxn_is_serialized (checks if transaction was serialized)
  - rbtxn_has_streamable_change (checks if transaction has streamable content)
- Called from (representative examples):
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md) (when processing new changes)

## Notes and Other Information
- Only processes partial changes when streaming is enabled (ReorderBufferCanStream returns true)
- Uses RBTXN_HAS_PARTIAL_CHANGE flag to mark transactions with incomplete changes
- TOAST inserts are considered partial until the main table insert/update completes with clear_toast_afterwards flag
- Speculative inserts remain partial until confirmed or aborted
- Automatically streams previously serialized transactions once their partial changes become complete
- Designed to prevent streaming incomplete changes while minimizing apply lag for delayed transactions
- All partial change tracking is done on the top-level transaction, not subtransactions