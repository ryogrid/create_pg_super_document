# computeDelta

## Location
[src/backend/access/transam/generic_xlog.c:228-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/generic_xlog.c#L228-L268)

## Overview
Computes the XLOG delta record needed to transform a current page into a target page and stores it in the PageData structure's delta field.

## Definition

```c
static void
computeDelta(PageData *pageData, Page curpage, Page targetpage)
```
## Detailed Description
This function serves as the high-level coordinator for generating WAL delta records that capture the differences between two PostgreSQL pages. It leverages the page structure's organization, specifically the lower and upper regions defined by the PageHeader, to efficiently compute deltas for only the meaningful portions of the page.

PostgreSQL pages have a specific layout with a lower region (containing line pointers and other metadata) and an upper region (containing tuple data), with free space in between. This function intelligently computes deltas for only these two active regions, ignoring the free space in the middle, which significantly reduces the amount of data that needs to be logged.

The function also includes debugging capability when WAL_DEBUG is enabled, allowing verification that the computed delta can correctly transform the current page into the target page.

## Parameters / Member Variables
- `*pageData`: Pointer to PageData structure where the computed delta will be stored
- `curpage`: Page pointer to the current state of the page (source)
- `targetpage`: Page pointer to the desired final state of the page (target)
## Dependencies
- Functions called/Symbols referenced:
  - PageData (struct type)
  - PageHeader (struct type for accessing page header fields)
  - [computeRegionDelta](computeRegionDelta.md) (called twice - for lower and upper regions at lines 238 and 242)
  - Page (typedef for page pointer)
  - BLCKSZ (constant for block size)
  - PGAlignedBlock (struct for aligned memory blocks, debug only)
  - [applyPageRedo](../a/applyPageRedo.md) (function to apply delta, debug only)
  - memcpy, memcmp (standard library functions, debug only)
  - elog (PostgreSQL logging function, debug only)
- Called from (representative examples):
  - [GenericXLogFinish](../G/GenericXLogFinish.md) (at line 370)

## Notes and Other Information
- This is a static function, only accessible within generic_xlog.c
- Resets deltaLen to 0 before computing, ensuring a clean delta buffer
- Leverages PostgreSQL's page layout by processing only the lower region (0 to pd_lower) and upper region (pd_upper to BLCKSZ)
- Includes comprehensive debugging support when WAL_DEBUG is enabled to verify delta correctness
- The two-region approach is an optimization that avoids logging the free space between pd_lower and pd_upper
- Part of PostgreSQL's generic WAL logging system for custom access methods
- The debug verification creates a temporary copy of the current page, applies the computed delta, and verifies the result matches the target page

## Simplified Source

```c
static void
computeDelta(PageData *pageData, Page curpage, Page targetpage)
{
    // Extract page boundaries from headers
    int targetLower = ((PageHeader) targetpage)->pd_lower;
    int targetUpper = ((PageHeader) targetpage)->pd_upper;
    int curLower = ((PageHeader) curpage)->pd_lower;
    int curUpper = ((PageHeader) curpage)->pd_upper;

    pageData->deltaLen = 0;

    // Compute delta for lower part of page (header and line pointers)
    computeRegionDelta(pageData, curpage, targetpage,
                       0, targetLower,
                       0, curLower);

    // Compute delta for upper part of page (tuple data)
    // Ignore free space between lower and upper regions
    computeRegionDelta(pageData, curpage, targetpage,
                       targetUpper, BLCKSZ,
                       curUpper, BLCKSZ);

#ifdef WAL_DEBUG
    // Verify delta correctness if debugging enabled
    if (XLOG_DEBUG) {
        PGAlignedBlock tmp;
        memcpy(tmp.data, curpage, BLCKSZ);
        applyPageRedo(tmp.data, pageData->delta, pageData->deltaLen);

        if (memcmp(tmp.data, targetpage, targetLower) != 0 ||
            memcmp(tmp.data + targetUpper, targetpage + targetUpper,
                   BLCKSZ - targetUpper) != 0)
            elog(ERROR, "result of generic xlog apply does not match");
    }
#endif
}
```