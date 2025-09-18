# heap_freetuple

## Location
src/backend/access/common/heaptuple.c: 1434 - 1451

## Overview
Frees the memory allocated for a HeapTuple structure by calling pfree() on the tuple pointer.

## Definition


## Detailed Description
The  function is a simple wrapper around PostgreSQL's memory management system that deallocates the memory occupied by a HeapTuple. This function is essential for preventing memory leaks when heap tuples are no longer needed. It simply calls  on the provided HeapTuple pointer, which releases the memory back to PostgreSQL's memory context system.

This function is used extensively throughout the PostgreSQL codebase whenever heap tuples need to be cleaned up after processing, whether in storage operations, catalog maintenance, replication, or general tuple manipulation.

## Parameters / Member Variables
- : A pointer to the HeapTuple structure to be freed. The tuple must have been previously allocated through PostgreSQL's memory management system.

## Dependencies
- Functions called/Symbols referenced:
  - pfree
- Called from (representative examples):
  - toast_save_datum
  - heap_insert
  - heap_delete
  - heap_update
  - ExtractReplicaIdentity
  - reform_and_rewrite_tuple
  - rewrite_heap_tuple
  - InsertOneTuple
  - CatalogTuplesMultiInsertWithInfo
  - ExecBRInsertTriggers
  - SPI_freetuple
  - RelationBuildDesc

## Notes and Other Information
- This is a fundamental memory management function used throughout PostgreSQL
- The function assumes the HeapTuple was allocated using PostgreSQL's memory context system
- It's critical to call this function to prevent memory leaks when heap tuples are no longer needed
- The function is used in both normal database operations and system catalog maintenance
- Used extensively in trigger execution, replication, and storage layer operations