# ReindexIsProcessingHeap

## Location
src/backend/catalog/index.c: 4058 - 4067

## Overview
The  function checks whether a specific heap relation is currently undergoing reindexing operations by comparing against a global tracking variable.

## Definition


## Detailed Description
This function provides a simple boolean check to determine if a heap relation identified by its OID is currently being processed during a reindex operation. It compares the provided heap OID against the global variable  that tracks which heap relation is currently undergoing reindexing. This mechanism helps prevent recursive reindexing operations and ensures proper coordination during index rebuilding processes.

The function is part of PostgreSQL's reindex coordination system that prevents issues that could arise from attempting to reindex a relation that is already being reindexed, or from using indexes that are in an inconsistent state during rebuilding.

## Parameters / Member Variables
- : Object identifier of the heap relation to check for active reindexing

## Dependencies
- Functions called/Symbols referenced:
  - currentlyReindexedHeap: Global variable tracking the currently reindexed heap relation
- Called from (representative examples):
  - Various index-related functions that need to avoid operations on currently reindexing heaps

## Notes and Other Information
- Returns true if the specified heap relation is currently being reindexed, false otherwise
- Part of the reindex coordination mechanism to prevent recursive operations
- Uses a simple global variable comparison for efficient checking
- Essential for maintaining consistency during concurrent index operations
- Helps coordinate with other reindex-related functions like SetReindexProcessing and ResetReindexProcessing