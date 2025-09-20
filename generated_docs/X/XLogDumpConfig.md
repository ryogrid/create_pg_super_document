# XLogDumpConfig

## Location
[src/bin/pg_waldump/pg_waldump.c:55-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L55-L81)

## Overview
XLogDumpConfig is a comprehensive configuration structure that controls all aspects of WAL (Write-Ahead Log) dump operations in pg_waldump, including display options, filtering criteria, and save operations.

## Definition

```c
typedef struct XLogDumpConfig
{
	/* display options */
	bool		quiet;
	bool		bkp_details;
	int			stop_after_records;
	int			already_displayed_records;
	bool		follow;
	bool		stats;
	bool		stats_per_record;

	/* filter options */
	bool		filter_by_rmgr[RM_MAX_ID + 1];
	bool		filter_by_rmgr_enabled;
	TransactionId filter_by_xid;
	bool		filter_by_xid_enabled;
	RelFileLocator filter_by_relation;
	bool		filter_by_extended;
	bool		filter_by_relation_enabled;
	BlockNumber filter_by_relation_block;
	bool		filter_by_relation_block_enabled;
	ForkNumber	filter_by_relation_forknum;
	bool		filter_by_fpw;

	/* save options */
	char	   *save_fullpage_path;
} XLogDumpConfig;
```
## Detailed Description
XLogDumpConfig serves as the central configuration hub for pg_waldump operations. It encompasses three main categories of settings: display options that control output format and verbosity, filter options that determine which WAL records to process based on various criteria, and save options for extracting specific data from WAL records.

This structure allows users to customize their WAL analysis experience, from simple record display to complex filtering based on resource managers, transaction IDs, relations, or specific blocks. The configuration is typically populated from command-line arguments and used throughout the WAL dump process.

## Parameters / Member Variables

- `quiet`: Suppresses verbose output, showing only essential information
- `bkp_details`: Controls whether to display detailed backup block information
- `stop_after_records`: Maximum number of records to display before stopping
- `already_displayed_records`: Counter tracking how many records have been displayed
- `follow`: Enables continuous monitoring mode, similar to 'tail -f'
- `stats`: Enables display of statistical summary information
- `stats_per_record`: Shows statistics for each individual record type
- `filter_by_rmgr[RM_MAX_ID + 1]`: Array of boolean flags for each resource manager type
- `filter_by_rmgr_enabled`: Master flag indicating if resource manager filtering is active
- `filter_by_xid`: Transaction ID to filter by when enabled
- `filter_by_xid_enabled`: Flag indicating if transaction ID filtering is active
- `filter_by_relation`: RelFileLocator specifying which relation to filter by
- `filter_by_extended`: Flag for extended filtering options
- `filter_by_relation_enabled`: Flag indicating if relation filtering is active
- `filter_by_relation_block`: Specific block number within a relation to filter by
- `filter_by_relation_block_enabled`: Flag indicating if block-level filtering is active
- `filter_by_relation_forknum`: Fork number (main, FSM, VM) for relation filtering
- `filter_by_fpw`: Flag to filter by full page writes
- `*save_fullpage_path`: Directory path where full page images should be saved
## Dependencies
- Functions called/Symbols referenced:
  - RM_MAX_ID (maximum resource manager ID constant)
  - TransactionId (PostgreSQL transaction identifier type)
  - [RelFileLocator](../R/RelFileLocator.md) (relation file locator structure)
  - BlockNumber (block number type)
  - [ForkNumber](../F/ForkNumber.md) (relation fork identifier type)
- Called from (representative examples):
  - [XLogDumpDisplayRecord](XLogDumpDisplayRecord.md)
  - [XLogDumpDisplayStats](XLogDumpDisplayStats.md)
  - [main](../m/main.md) (pg_waldump)

## Notes and Other Information
- This structure is exclusively used by the pg_waldump utility for WAL analysis and debugging
- The filtering capabilities allow for very granular control over which WAL records are processed
- The resource manager filter array covers all possible resource manager types in PostgreSQL
- The follow mode enables real-time WAL monitoring for debugging active systems
- Full page write extraction can be useful for forensic analysis and debugging
- Located in src/bin/pg_waldump/pg_waldump.c:55-81