# MultiXactOffsetPagePrecedes

## Location
[src/backend/access/transam/multixact.c:3269-3288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3269-L3288)

## Overview
MultiXactOffsetPagePrecedes determines whether one MultiXact offset page number is "older" than another for truncation purposes, using MultiXact ID precedence logic.

## Definition
static bool MultiXactOffsetPagePrecedes(int64 page1, int64 page2)

## Detailed Description
This function implements the page ordering logic for MultiXact offset pages, analogous to CLOGPagePrecedes() for commit log pages. It converts page numbers back to their corresponding MultiXact ID ranges and uses the established MultiXactIdPrecedes() logic to determine precedence.

The function works by converting each page number to the MultiXact ID range it represents, then checking if the first page's entire range precedes the second page's range. This ensures consistent ordering semantics across the MultiXact offset SLRU system, which is critical for proper truncation operations.

The offset calculation adds FirstMultiXactId + 1 to account for the base MultiXact ID offset, and the precedence check ensures that page1's entire range (including the last MultiXact ID on that page) precedes page2's first MultiXact ID.

## Parameters / Member Variables
- `page1`: First page number to compare
- `page2`: Second page number to compare

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactIdPrecedes](MultiXactIdPrecedes.md)
  - MULTIXACT_OFFSETS_PER_PAGE
  - FirstMultiXactId
- Called from (representative examples):
  - [MultiXactShmemInit](MultiXactShmemInit.md) (as function pointer in SLRU control structure)
  - debug_elog6 (debugging/logging context)

## Notes and Other Information
- Analogous to CLOGPagePrecedes() but for MultiXact offset pages
- Uses translational symmetry property of MultiXactIdPrecedes()
- Critical for SLRU truncation operations and page ordering
- Converts page numbers to MultiXact ID ranges for comparison
- Ensures entire page ranges are considered, not just starting IDs
- Part of the SLRU control structure function pointer interface