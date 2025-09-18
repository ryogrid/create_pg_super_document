# ReorderBufferLargestStreamableTopTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 3723 - 3766

## Overview
Identifies the largest streamable top-level transaction from the reorder buffer that can be evicted via streaming, focusing only on transactions with base snapshots and complete changes.

## Definition


## Detailed Description
ReorderBufferLargestStreamableTopTXN is an optimized function that finds the largest top-level transaction suitable for streaming eviction. Unlike ReorderBufferLargestTXN which considers all transactions, this function specifically targets transactions that meet streaming requirements.

The function iterates through the limited set of top-level transactions that have base snapshots (maintained in  list) rather than scanning the entire transaction heap. This optimization is based on the fact that only transactions with base snapshots can be decoded and streamed.

Key selection criteria for a transaction:
- Must be a top-level transaction (not a subtransaction)
- Must have a base snapshot set
- Must have a positive total size
- Must not contain incomplete/partial changes
- Must contain streamable changes

The function includes extensive commentary explaining why transactions with incomplete changes are currently skipped, noting the complexity that would be involved in partially streaming such transactions (requiring partial file truncation, LSN tracking, and subxact state management).

## Parameters / Member Variables
- : Pointer to the ReorderBuffer structure containing the transaction lists to search

## Dependencies
- Functions called/Symbols referenced:
  - dlist_foreach (macro for iterating through doubly-linked lists)
  - dlist_container (macro for extracting container structure from list node)
  - rbtxn_is_known_subxact (checks if transaction is a known subtransaction)
  - rbtxn_has_partial_change (checks if transaction has incomplete changes)  
  - rbtxn_has_streamable_change (checks if transaction has changes suitable for streaming)
- Called from (representative examples):
  - ReorderBufferCheckMemoryLimit (for identifying transactions to stream during memory pressure)

## Notes and Other Information
- This is a static function, only accessible within reorderbuffer.c
- Optimized alternative to ReorderBufferLargestTXN for streaming scenarios
- Only considers top-level transactions with base snapshots, significantly reducing search scope
- Excludes transactions with partial changes to avoid complex partial streaming logic
- Part of the streaming-based memory pressure management system
- The function may return NULL if no suitable streamable transaction is found
- Contains detailed design rationale for current limitations and potential future optimizations
- Memory accounting for subtransactions is always 0 when streaming is enabled