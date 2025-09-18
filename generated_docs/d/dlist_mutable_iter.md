# dlist_mutable_iter

## Location
src/include/lib/ilist.h: 198 - 203

## Overview
The  structure provides iterator state for safely traversing doubly-linked lists while allowing limited modifications during iteration.

## Definition


## Detailed Description
The  structure extends the basic iteration capabilities by allowing controlled modifications to the list during traversal. Unlike the read-only , this iterator supports deletion of the current node while maintaining safe iteration state.

The key safety feature is the inclusion of a  pointer that stores the next node to be visited. This allows the current node to be safely deleted without losing track of where the iteration should continue. However, the iterator has strict limitations: only the current node may be modified or deleted, and insertion or deletion of adjacent nodes is prohibited.

This iterator is used by  and  macros, providing a standardized way to perform list cleanup operations, selective deletions, and other modifications that require traversing and potentially altering the list structure.

## Parameters / Member Variables
- : Pointer to the current  being processed during iteration
- : Pointer to the next  that will be processed, stored separately to handle current node deletion safely
- : Pointer to the last  that will be processed in this iteration, used to determine when to stop

## Dependencies
- Functions called/Symbols referenced:
  -  (used for cur, next, and end members)
- Called from (representative examples):
  -  (macro for safe modification during iteration)
  -  (dclist variant)
  - Used extensively in PostgreSQL for cleanup operations including memory management, lock release, cache invalidation, and transaction cleanup

## Notes and Other Information
- Allows safe deletion of the current node during iteration, unlike 
- The  pointer is critical for maintaining iteration state when the current node is deleted
- Strictly prohibits modification of adjacent nodes - only the current node may be altered
- Essential for implementing safe cleanup and selective deletion patterns throughout PostgreSQL
- Used in critical subsystems including transaction processing, memory management, and replication for safe list maintenance operations