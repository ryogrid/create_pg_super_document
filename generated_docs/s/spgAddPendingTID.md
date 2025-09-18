# spgAddPendingTID

## Location
src/backend/access/spgist/spgvacuum.c: 63 - 88

## Overview
Adds a TID (Tuple Identifier) to the pending list during SP-GiST vacuum operations, but only if the TID is not already present in the list.

## Definition


## Detailed Description
This function maintains a list of pending TIDs during SP-GiST vacuum operations. It performs a linear search through the existing pending list to check for duplicates before adding new entries. New items are always appended at the end of the list, which ensures that scans of the list don't miss items added during the scan. This is crucial for maintaining consistency during concurrent vacuum operations.

The function allocates memory for new pending items and initializes them with the provided TID, setting the 'done' flag to false and the 'next' pointer to NULL.

## Parameters / Member Variables
- : Pointer to spgBulkDeleteState structure containing the vacuum state information, including the pendingList
- : ItemPointer (TID) to be added to the pending list

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerEquals: Compares two ItemPointer values for equality
  - palloc: PostgreSQL memory allocation function
  - spgBulkDeleteState: Structure containing vacuum state
  - spgVacPendingItem: Structure representing a pending vacuum item
- Called from (representative examples):
  - vacuumLeafPage: Calls when processing leaf pages during vacuum
  - spgprocesspending: Calls when processing pending items during vacuum operations

## Notes and Other Information
- This is a static function, meaning it's only accessible within the spgvacuum.c file
- The function prevents duplicate entries by searching the entire list before adding
- Memory allocation uses palloc, which will throw an error if allocation fails
- The append-only nature of the list ensures scan consistency during vacuum operations
- The function is part of the SP-GiST (Space-Partitioned Generalized Search Tree) vacuum implementation