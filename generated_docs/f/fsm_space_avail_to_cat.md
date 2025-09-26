# fsm_space_avail_to_cat

## Location
[src/backend/storage/freespace/freespace.c:392-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/freespace/freespace.c#L392-L417)

## Overview
fsm_space_avail_to_cat converts a byte count of available free space into a category code (0-255) used internally by the Free Space Map for efficient storage and retrieval.

## Definition
static uint8 fsm_space_avail_to_cat(Size avail)

## Detailed Description
This internal function maps available free space (measured in bytes) to a category code that can be stored efficiently in the Free Space Map. The FSM uses a categorization system to compress free space information into single-byte values, trading some precision for space efficiency.

The function implements the following mapping:
- Free space >= MaxFSMRequestSize bytes maps to category 255 (the highest category)
- Other values are divided by FSM_CAT_STEP to determine the category
- Categories above 254 are capped at 254, reserving 255 for the maximum case
- The result is a category code from 0 to 255, where higher values represent more free space

This categorization allows the FSM to store approximate free space information in a compact format while still providing useful granularity for space allocation decisions.

## Parameters / Member Variables
- : The number of bytes of available free space to convert to a category

## Dependencies
- Functions called/Symbols referenced:
  - MaxFSMRequestSize (maximum free space request size constant)
  - FSM_CAT_STEP (step size for category calculation)
- Called from (representative examples):
  - RecordAndGetPageWithFreeSpace (src/backend/storage/freespace/freespace.c:157)
  - RecordPageWithFreeSpace (src/backend/storage/freespace/freespace.c:196)
  - XLogRecordPageWithFreeSpace (src/backend/storage/freespace/freespace.c:214)

## Notes and Other Information
- This is a static internal function, not exposed in the public API
- Assumes that avail < BLCKSZ (less than block size), enforced by assertion
- Category 255 is reserved for MaxFSMRequestSize bytes or more
- Categories 0-254 provide graduated resolution for smaller free space amounts
- The categorization is lossy compression - exact byte counts cannot be recovered
- Essential for FSM's space-efficient storage of free space information
- Located in src/backend/storage/freespace/freespace.c:386-411