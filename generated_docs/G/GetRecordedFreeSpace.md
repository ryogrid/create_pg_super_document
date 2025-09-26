# GetRecordedFreeSpace

## Location
[src/backend/storage/freespace/freespace.c:244-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L244-L274)

## Overview
GetRecordedFreeSpace retrieves the amount of free space recorded in the FSM for a specific page, providing a way to query stored free space information without searching for pages.

## Definition
```c
Size GetRecordedFreeSpace(Relation rel, BlockNumber heapBlk)
```

## Detailed Description
This function serves as a query interface for the Free Space Map, allowing callers to retrieve the currently recorded free space amount for a specific page. Unlike the search functions that look for pages with sufficient space, this function simply returns what the FSM believes about a particular page's free space. The function handles the low-level details of locating the correct FSM page, reading the appropriate slot, and converting the categorized free space value back to a byte count.

The function includes proper error handling for cases where the FSM page doesn't exist, returning 0 to indicate no recorded free space information is available. This is important during system initialization or for newly extended relations where FSM pages may not yet exist.

## Parameters / Member Variables
- `rel`: The relation containing the page to query
- `heapBlk`: The block number of the page whose recorded free space is requested

## Dependencies
- Functions called/Symbols referenced:
  - FSMAddress
  - fsm_get_location
  - fsm_readbuf
  - fsm_get_avail
  - ReleaseBuffer
  - fsm_space_cat_to_avail
- Called from (representative examples):
  - lazy_scan_new_or_empty

## Notes and Other Information
- Returns 0 if the FSM page doesn't exist or is invalid
- Converts FSM categories back to approximate byte counts
- Primarily used for diagnostic and optimization purposes
- Does not modify FSM state, only queries existing information
- Less commonly used compared to the search and update functions
- Part of PostgreSQL's Free Space Map public API
- Located in src/backend/storage/freespace/freespace.c:244-274