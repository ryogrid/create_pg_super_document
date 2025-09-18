# CommitTsPagePrecedes

## Location
src/backend/access/transam/commit_ts.c: 977 - 995

## Overview
Determines whether one commit timestamp SLRU page "precedes" another for truncation purposes, similar to CLOGPagePrecedes but with special handling for commit timestamp page boundaries.

## Definition
```c
static bool CommitTsPagePrecedes(int64 page1, int64 page2)
```

## Detailed Description
This function decides whether a commit timestamp page number is "older" for truncation purposes and is analogous to CLOGPagePrecedes(). The function handles the unique characteristics of commit timestamp SLRU pages where (1 << 31) % COMMIT_TS_XACTS_PER_PAGE == 128 at default BLCKSZ, which introduces differences compared to CLOG and other SLRUs. Due to this mathematical property, the function may occasionally return false for one page that is actually expendable, representing a wider (yet still negligible) version of the truncation opportunity that CLOGPagePrecedes() cannot recognize. The function converts page numbers to transaction IDs and uses TransactionIdPrecedes() to determine the ordering relationship.

## Parameters / Member Variables
- `page1`: The first page number to compare (typically the potentially older page)
- `page2`: The second page number to compare (typically the potentially newer page)

## Dependencies
- Functions called/Symbols referenced:
  - COMMIT_TS_XACTS_PER_PAGE
  - FirstNormalTransactionId
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
- Called from (representative examples):
  - CommitTsShmemInit (as a function pointer for SLRU operations)

## Notes and Other Information
- Static function, internal to commit_ts.c
- The function includes extensive comments explaining the mathematical complexities of commit timestamp page boundaries
- At xidStopLimit, there can be two possible counts of page boundaries between oldestXact and the latest XID, depending on oldestXact's position within its page
- The function converts page numbers to transaction IDs by multiplying by COMMIT_TS_XACTS_PER_PAGE and adding FirstNormalTransactionId + 1
- Uses double TransactionIdPrecedes() calls to ensure both the start and end of the page range are properly ordered
- Part of the commit timestamp SLRU management system for determining which pages can be safely truncated