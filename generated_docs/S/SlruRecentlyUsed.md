# SlruRecentlyUsed

## Location
src/backend/access/transam/slru.c: 1120 - 1165

## Overview
Marks a buffer slot as "most recently used" in the SLRU cache by updating its LRU counter, optimizing the cache replacement algorithm.

## Definition


## Detailed Description
SlruRecentlyUsed is an inline function that implements the LRU (Least Recently Used) tracking mechanism for SLRU buffer slots. It updates the LRU counter for a specific slot to mark it as recently accessed, which helps the cache replacement algorithm make better decisions about which pages to evict. The function uses a bank-based approach where buffer slots are organized into banks, and each bank maintains its own LRU counter. The function includes an optimization to avoid unnecessary counter updates when the same page is accessed consecutively multiple times.

## Parameters / Member Variables
- `shared`: Pointer to the shared SLRU control structure containing cache state and counters
- `slotno`: The slot number to mark as recently used

## Dependencies
- Functions called/Symbols referenced:
  - SlotGetBankNumber
- Constants used:
  - SLRU_PAGE_EMPTY
- Types used:
  - SlruShared
- Called from:
  - SimpleLruZeroPage  
  - SimpleLruReadPage
  - SimpleLruReadPage_ReadOnly

## Notes and Other Information
- The function includes an optimization that suppresses redundant LRU counter updates when the same page is accessed consecutively, reducing counter wrap-around probability
- Designed to be safe for concurrent execution by multiple processes within SimpleLruReadPage_ReadOnly()
- Uses atomic int reads and writes assumption for thread safety
- Potential race conditions may cause counters to be reset to lower values, but this only affects eviction optimality, not correctness
- The bank-based organization helps distribute LRU counter management across multiple banks to reduce contention
- Assert check ensures the slot is not empty before updating LRU information
- Counter wrap-around protection helps maintain the relative ordering of page access times