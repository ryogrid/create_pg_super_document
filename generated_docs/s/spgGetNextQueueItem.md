# spgGetNextQueueItem

## Location
src/backend/access/spgist/spgscan.c: 746 - 754

## Overview
Retrieves the next item from an ordered scan queue during SP-GiST index scanning, returning NULL when the index is exhausted.

## Definition


## Detailed Description
This function serves as a queue management utility for SP-GiST (Space-partitioned Generalized Search Tree) index scans. It operates on a priority queue (pairing heap) that maintains search items in order during index traversal. The function performs a simple but critical role: it checks if the scan queue is empty and either returns the next item to process or signals completion of the scan by returning NULL. The caller is responsible for freeing the returned item.

## Parameters / Member Variables
- : SpGistScanOpaque structure containing the scan state, including the scanQueue (pairing heap) that stores search items in priority order

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_is_empty
  - pairingheap_remove_first
  - SpGistScanOpaque
  - SpGistSearchItem
- Called from (representative examples):
  - spgWalk

## Notes and Other Information
- This is a static function internal to spgscan.c
- The function assumes the caller will properly free the returned SpGistSearchItem
- The pairing heap implementation ensures items are returned in the correct order for the scan
- Located at src/backend/access/spgist/spgscan.c:746-754