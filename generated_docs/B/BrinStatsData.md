# BrinStatsData

## Location
[src/include/access/brin.h:32-36](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/brin.h#L32-L36)

## Overview
BrinStatsData is a PostgreSQL data structure that holds statistical information about BRIN indexes for use by the query planner in cost estimation and optimization decisions.

## Definition


## Detailed Description
BrinStatsData encapsulates essential statistical information about BRIN (Block Range Index) structures that the PostgreSQL query planner needs for accurate cost estimation. This structure is populated by reading metadata from BRIN indexes and is used primarily in the brincostestimate() function to calculate the cost of using a BRIN index for query execution.

The structure contains two critical metrics:
1. **pagesPerRange**: Indicates how many heap pages are summarized by each BRIN index tuple, which directly affects the granularity of the index
2. **revmapNumPages**: Represents the number of pages occupied by the reverse map structure, which is essential for calculating the sequential I/O cost during index startup

This data is used by the query planner to make informed decisions about whether to use a BRIN index for a given query, comparing its estimated cost against other available indexes or sequential scans.

## Parameters / Member Variables
- : Number of heap table pages that each BRIN index tuple summarizes (affects index selectivity and granularity)
- : Number of pages occupied by the BRIN reverse map structure (used for startup cost calculations)

## Dependencies
- Functions called/Symbols referenced:
  - BlockNumber (type dependency)
- Called from (representative examples):
  - [brinGetStats](../b/brinGetStats.md)() function in src/backend/access/brin/brin.c:1639
  - [brincostestimate](../b/brincostestimate.md)() function in src/backend/utils/adt/selfuncs.c:8053

## Notes and Other Information
- The structure is defined in src/include/access/brin.h:32-36
- Primary purpose is to provide statistical data for query planner cost estimation
- Data is populated by brinGetStats() which reads from the BRIN index metadata page
- Used extensively in brincostestimate() to calculate index startup costs, total costs, and selectivity estimates
- The revmapNumPages is calculated as (lastRevmapPage - 1) from the index metadata
- For hypothetical indexes, default values are used when actual statistics are not available
- Critical for determining whether a BRIN index scan is more cost-effective than alternative access methods
- The pagesPerRange value directly impacts the estimated number of ranges that will be examined during index scans