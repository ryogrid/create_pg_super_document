# PageSetLSN

## Location
[src/include/storage/bufpage.h:389-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufpage.h#L389-L394)

## Overview
Sets the Log Sequence Number (LSN) in a page header, updating the WAL position marker to track the last modification made to the page for recovery and consistency purposes.

## Definition
static inline void PageSetLSN(Page page, XLogRecPtr lsn)

## Detailed Description
PageSetLSN updates the Log Sequence Number (LSN) stored in a page's header to reflect the WAL position of the most recent modification. This function is critical for maintaining PostgreSQL's Write-Ahead Logging consistency, as it ensures that each modified page correctly tracks the last WAL record that affected it.

The function uses PageXLogRecPtrSet to properly store the LSN value in the page header's pd_lsn field. This LSN information is essential during crash recovery to determine whether changes need to be replayed from the WAL, ensuring that no committed transactions are lost and that the database remains in a consistent state.

This function is typically called after making modifications to a page and before writing corresponding WAL records, maintaining the strict ordering required by PostgreSQL's recovery mechanism.

## Parameters / Member Variables
- : A Page pointer to the page whose LSN should be updated
- : An XLogRecPtr value representing the WAL position to store in the page header

## Dependencies
- Functions called/Symbols referenced:
  - PageXLogRecPtrSet (function to properly store XLogRecPtr in page header)
  - PageHeader (type cast for accessing page header structure)
- Called from (representative examples):
  - Currently shows no direct references, but typically used in WAL-logged operations
  - Likely used in low-level page modification routines during WAL record processing
  - May be used indirectly through buffer management functions

## Notes and Other Information
- Essential for maintaining WAL consistency and ensuring proper crash recovery
- Must be called whenever a page is modified to update the LSN tracking
- The LSN value should correspond to the WAL record that describes the page modification  
- Critical for determining page ordering during recovery operations
- Used to ensure that pages are consistent with their corresponding WAL records
- The function provides atomic updates to the LSN field in the page header
- Essential component of PostgreSQL's durability and consistency guarantees
- Typically used in conjunction with WAL record generation and buffer management operations