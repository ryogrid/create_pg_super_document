# ReorderBufferAbort

## Location
src/backend/replication/logical/reorderbuffer.c: 2968 - 3013

## Overview
Aborts a transaction that possibly has previous changes, purging the transaction and its contents from memory and disk.

## Definition


## Detailed Description
ReorderBufferAbort handles the cleanup of transactions that have actively aborted (i.e., have produced an abort record). This function is designed to be called first for subtransactions and then for the toplevel transaction ID. 

The function performs several key operations:
1. Looks up the transaction by XID in the reorder buffer
2. Records the abort time in the transaction structure
3. For streamed transactions, notifies the remote node about the abort via the stream_abort callback
4. Handles cache invalidation for streamed transactions that may have loaded cache entries during decoding
5. Sets the final LSN and cleans up all transaction data

This is distinct from ReorderBufferAbortOld() which handles implicitly aborted transactions, and ReorderBufferForget() which handles committed transactions that are no longer of interest.

## Parameters / Member Variables
- : The ReorderBuffer instance managing the transaction
- : Transaction ID of the transaction to abort
- : Log Sequence Number where the abort occurred
- : Timestamp when the transaction was aborted

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - rbtxn_is_streamed
  - [ReorderBufferImmediateInvalidation](ReorderBufferImmediateInvalidation.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
- Called from (representative examples):
  - [DecodeAbort](../D/DecodeAbort.md) (in decode.c)

## Notes and Other Information
- Only handles transactions that have actively aborted with an abort record
- For streamed transactions, performs additional cleanup including remote notification and cache invalidation
- Must be called for subtransactions before the toplevel transaction
- Unknown transactions (NULL lookup result) are safely ignored
- The function ensures proper cleanup of both memory and disk-based transaction data