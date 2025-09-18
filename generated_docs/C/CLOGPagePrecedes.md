# CLOGPagePrecedes

## Location
[src/backend/access/transam/clog.c:1055-1073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L1055-L1073)

## Overview
CLOGPagePrecedes determines whether one CLOG page number is "older" than another for truncation purposes, handling PostgreSQL's wraparound transaction ID arithmetic correctly.

## Definition
```c
static bool CLOGPagePrecedes(int64 page1, int64 page2)
```

## Detailed Description
CLOGPagePrecedes is a comparison function used by the SLRU (Simple Least Recently Used) system to determine which CLOG pages are older and can be truncated. The function must correctly handle PostgreSQL's wraparound transaction ID arithmetic, where transaction IDs wrap around after reaching the maximum value.

The function converts page numbers to representative transaction IDs by multiplying by CLOG_XACTS_PER_PAGE and adding an offset (FirstNormalTransactionId + 1) to ensure all compared transaction IDs are normal XIDs. This offset is crucial for handling page 0 and the page preceding page 0 correctly.

The function returns true if page1 precedes page2, meaning that both the first transaction ID of page1 precedes the first transaction ID of page2, AND the first transaction ID of page1 also precedes the last transaction ID of page2. This dual check ensures that the entire range of page1 precedes the entire range of page2.

## Parameters / Member Variables
- `page1`: The first CLOG page number to compare
- `page2`: The second CLOG page number to compare

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (called twice)
- Global variables/constants accessed:
  - CLOG_XACTS_PER_PAGE
  - FirstNormalTransactionId
- Referenced by:
  - XactCtl (src/backend/access/transam/clog.c:114)
  - [CLOGShmemInit](CLOGShmemInit.md) (src/backend/access/transam/clog.c:810)

## Notes and Other Information
- Static function used internally within the CLOG module
- Essential for SLRU truncation operations to work correctly with wraparound arithmetic
- The offset (FirstNormalTransactionId + 1) prevents issues with permanent transaction IDs
- Does not optimize for the edge case where oldestXact-2^31 is the first XID of a page
- Used as a callback function in the SLRU control structure (XactCtl)
- The dual TransactionIdPrecedes check ensures the entire page1 range precedes page2 range
- Critical for maintaining CLOG consistency during truncation operations