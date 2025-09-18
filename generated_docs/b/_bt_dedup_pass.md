# _bt_dedup_pass

## Location
src/backend/access/nbtree/nbtdedup.c: 58 - 306

## Overview
Performs a B-tree deduplication pass to merge duplicate index tuples into posting list tuples, freeing up page space to potentially avoid page splits.

## Definition


## Detailed Description
This function implements the core B-tree deduplication algorithm that scans through index tuples on a page and merges duplicates into posting list tuples to save space. The function uses two different strategies:

1. **General deduplication**: Merges as many duplicates as possible to maximize space savings
2. **Single value strategy**: For pages full of tuples with a single value, leaves some tuples untouched at the end to prepare for anticipated page splits

When called after a failed , the goal is to prevent page splits entirely by buying more time. The function will only proceed if it can free at least  bytes (plus line pointer overhead).

The deduplication process creates a new temporary page, copies tuples while merging duplicates, and then replaces the original page content. All changes are logged for WAL replay.

## Parameters / Member Variables
- : The index relation being processed
- : Buffer containing the page to be deduplicated  
- : New index tuple that needs to be inserted (used for space calculations)
- : Size of the new item in bytes (MAXALIGNED, excluding line pointer)
- : If true, indicates this call follows a failed bottom-up deletion pass

## Dependencies
- Functions called/Symbols referenced:
  - : Determines if single value strategy should be applied
  - : Initializes a new pending posting list
  - : Attempts to add tuple's heap TIDs to pending list
  - : Finalizes pending posting list and adds to page
  - : Adjusts posting list size for single value strategy
  - : Creates temporary page copy
  - : Replaces original page with modified version

- Called from (representative examples):
  - : Main deduplication entry point during insertion

## Notes and Other Information
- The function implements a "single value" strategy for pages containing many tuples of the same value, leaving some tuples unmerged to optimize for future page splits
- Space calculations include both tuple data and line pointer overhead
- The function clears the BTP_HAS_GARBAGE flag since heapkeyspace indexes don't use it
- WAL logging ensures crash recovery can replay the deduplication operation
- If no deduplication intervals are created, the function returns early without modifying the page
- The maxpostingsize is limited to 1/6 of a page to ensure good split points for pages with many duplicates