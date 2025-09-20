# XLogStats

## Location
[src/include/access/xlogstats.h:28-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogstats.h#L28-L37)

## Overview
XLogStats is a comprehensive structure for collecting and analyzing WAL (Write-Ahead Log) statistics in PostgreSQL, providing detailed breakdowns by resource manager and record type.

## Definition

```c
typedef struct XLogStats
{
	uint64		count;
#ifdef FRONTEND
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
#endif
	XLogRecStats rmgr_stats[RM_MAX_ID + 1];
	XLogRecStats record_stats[RM_MAX_ID + 1][MAX_XLINFO_TYPES];
} XLogStats;
```
## Detailed Description
XLogStats serves as the primary container for comprehensive WAL statistics collection in PostgreSQL. This structure provides a hierarchical view of WAL activity, tracking overall counts as well as detailed breakdowns by resource manager type and individual record types.

The structure contains two main statistical arrays: rmgr_stats for per-resource-manager statistics and record_stats for per-record-type statistics within each resource manager. This two-level hierarchy allows for both high-level analysis (which resource managers are most active) and detailed analysis (which specific record types are most common within each resource manager).

In frontend applications (like pg_waldump), additional fields track the WAL position range being analyzed, providing context for the statistical data collected.

## Parameters / Member Variables
- `count`: Total number of WAL records processed across all types
- `startptr`: (FRONTEND only) Starting WAL position for the analysis range
- `endptr`: (FRONTEND only) Ending WAL position for the analysis range
- `rmgr_stats[RM_MAX_ID + 1]`: Array of XLogRecStats indexed by resource manager ID, containing per-resource-manager statistics
- `record_stats[RM_MAX_ID + 1][MAX_XLINFO_TYPES]`: Two-dimensional array of XLogRecStats indexed by [resource manager ID][record type], containing detailed per-record-type statistics
## Dependencies
- Functions called/Symbols referenced:
  - FRONTEND (preprocessor conditional compilation flag)
  - RM_MAX_ID (maximum resource manager ID constant)
  - [XLogRecStats](XLogRecStats.md) (member structure type)
  - MAX_XLINFO_TYPES (maximum number of record info types)
- Called from (representative examples):
  - [XLogRecStoreStats](XLogRecStoreStats.md) (function that updates statistics in this structure)
  - [XLogDumpDisplayStats](XLogDumpDisplayStats.md) (pg_waldump function that displays these statistics)
  - [main](../m/main.md) (pg_waldump main function that processes WAL files)

## Notes and Other Information
- This structure is defined in src/include/access/xlogstats.h as the central component of PostgreSQL's WAL statistics system
- The RM_MAX_ID constant is defined as UINT8_MAX (255), allowing for up to 256 different resource manager types
- MAX_XLINFO_TYPES is defined as 16, providing 16 different record type categories per resource manager
- The conditional compilation with FRONTEND allows the same structure to be used in both backend and frontend tools, with frontend tools having additional positioning information
- Primarily used by pg_waldump for analyzing WAL file contents and generating statistical reports
- The two-level statistics hierarchy (resource manager + record type) provides flexibility for both broad performance analysis and detailed debugging
- Resource managers in PostgreSQL handle different types of database operations (heap operations, btree operations, transaction operations, etc.)
- The structure supports PostgreSQL's pluggable resource manager architecture by using arrays sized to accommodate all possible resource manager IDs