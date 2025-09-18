# brinGetStats

## Location
src/backend/access/brin/brin.c: 1639 - 1659

## Overview
Fetches statistical data from a BRIN index's metadata page and populates the provided BrinStatsData structure.

## Definition


## Detailed Description
This function reads the metadata page of a BRIN index to extract essential statistical information. It accesses the first block of the index (the metadata page), reads the metadata structure, and extracts key statistics including the number of pages per range and the number of revmap pages. The function handles proper buffer locking to ensure safe concurrent access to the metadata page.

## Parameters / Member Variables
- : The BRIN index relation from which to extract statistics
- : Pointer to a BrinStatsData structure that will be populated with the index statistics

## Dependencies
- Functions called/Symbols referenced:
  - ReadBuffer
  - LockBuffer 
  - BufferGetPage
  - PageGetContents
  - UnlockReleaseBuffer
  - BRIN_METAPAGE_BLKNO (constant)
  - BUFFER_LOCK_SHARE (constant)
- Types referenced:
  - BrinStatsData
  - BrinMetaPageData
- Called from (representative examples):
  - brincostestimate
  - BrinGetAutoSummarize

## Notes and Other Information
- The function uses shared buffer locking to safely read the metadata page
- Statistics extracted include pagesPerRange and revmapNumPages which are critical for BRIN index cost estimation
- The revmap page count is calculated as (lastRevmapPage - 1) from the metadata
- This function is typically used by the query planner to estimate costs for BRIN index scans