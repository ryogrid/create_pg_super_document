# XLogRecStats

## Location
[src/include/access/xlogstats.h:21-26](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlogstats.h#L21-L26)

## Overview
XLogRecStats is a structure used to collect and store statistical information about individual WAL (Write-Ahead Log) records in PostgreSQL, tracking count, data length, and full page image length.

## Definition

```c
typedef struct XLogRecStats
{
	uint64		count;
	uint64		rec_len;
	uint64		fpi_len;
} XLogRecStats;
```
## Detailed Description
XLogRecStats serves as a statistical data container for WAL record analysis and monitoring. This structure is fundamental to PostgreSQL's WAL statistics collection system, providing granular metrics about WAL records. The structure tracks three key metrics: the number of records processed, the total length of record data (excluding full page images), and the total length of full page image data.

This structure is primarily used within the larger XLogStats framework to provide detailed breakdowns of WAL activity by resource manager type and record type. Each XLogRecStats instance represents accumulated statistics for a specific category of WAL records, enabling detailed performance analysis and monitoring of WAL activity patterns.

## Parameters / Member Variables
- `count`: Number of WAL records processed/encountered of this type
- `rec_len`: Total length in bytes of the record data (excluding full page images)
- `fpi_len`: Total length in bytes of full page image (FPI) data associated with these records
## Dependencies
- Functions called/Symbols referenced: (This is a data structure with no direct function calls)
- Called from (representative examples):
  - [XLogStats](XLogStats.md) (used as member in arrays within XLogStats structure)
  - [XLogRecStoreStats](XLogRecStoreStats.md) (indirectly through XLogStats structure)

## Notes and Other Information
- This structure is defined in src/include/access/xlogstats.h as part of the WAL statistics subsystem
- The structure separates record data length from full page image length to provide insight into different types of WAL overhead
- Full page images (FPIs) are complete copies of database pages stored in WAL records, typically after a checkpoint, and represent a significant portion of WAL volume
- The structure is used in both per-resource-manager statistics and per-record-type statistics within the broader XLogStats framework
- Introduced as part of PostgreSQL's WAL analysis and monitoring capabilities, particularly useful for pg_waldump and similar tools