# _bt_dedup_save_htid

## Location
src/backend/access/nbtree/nbtdedup.c: 484 - 554

## Overview
Attempts to save heap TIDs from an index tuple into the current pending posting list, checking size limits and merging duplicates where possible.

## Definition


## Detailed Description
This function is responsible for the core duplicate detection and merging logic during deduplication. It attempts to add heap TIDs from a given index tuple to the current pending posting list managed by the deduplication state.

The function performs several key operations:
1. Extracts heap TIDs from the input tuple (single TID for regular tuples, multiple TIDs for existing posting lists)
2. Calculates the size of a merged posting list tuple that would include both existing and new TIDs
3. Checks against the maxpostingsize limit to prevent excessively large posting lists
4. If the size limit allows, copies the new heap TIDs to the pending list and updates state counters

For the single value strategy, the function tracks when posting lists become large (>50 TIDs) to help manage page splitting behavior.

## Parameters / Member Variables
- : Deduplication state containing the current pending posting list and size limits
- : Index tuple whose heap TIDs should be added to the pending posting list

## Dependencies
- Functions called/Symbols referenced:
  - : Verifies tuple is not a pivot tuple
  - : Determines if tuple contains multiple heap TIDs
  - : Gets count of heap TIDs in posting list
  - : Extracts heap TID array from posting list
  - : Calculates tuple size for space accounting

- Called from (representative examples):
  - : Called to test if tuples can be merged during deduplication
  - : Called to group duplicate tuples for bottom-up deletion
  - : Called during index build to merge duplicates
  - : Called during WAL replay

## Notes and Other Information
- Returns true if the heap TIDs were successfully added, false if size limits prevented merging
- The size calculation must match the logic used in  for consistency
- Uses MAXALIGN for proper tuple alignment in size calculations
- The 50-TID threshold for nmaxitems counting is somewhat arbitrary but helps with single value strategy
- When size limits are exceeded, the caller typically finalizes the current pending list and starts a new one
- Physical size tracking includes both tuple data and line pointer overhead for accurate space accounting
- The function only works with leaf tuples, never pivot tuples (enforced by assertion)