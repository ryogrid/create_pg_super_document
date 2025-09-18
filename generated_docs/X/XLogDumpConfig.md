# XLogDumpConfig

## Location
src/bin/pg_waldump/pg_waldump.c: 55 - 81

## Overview
XLogDumpConfig is a comprehensive configuration structure that controls all aspects of WAL (Write-Ahead Log) dump operations in pg_waldump, including display options, filtering criteria, and save operations.

## Definition


## Detailed Description
XLogDumpConfig serves as the central configuration hub for pg_waldump operations. It encompasses three main categories of settings: display options that control output format and verbosity, filter options that determine which WAL records to process based on various criteria, and save options for extracting specific data from WAL records.

This structure allows users to customize their WAL analysis experience, from simple record display to complex filtering based on resource managers, transaction IDs, relations, or specific blocks. The configuration is typically populated from command-line arguments and used throughout the WAL dump process.

## Parameters / Member Variables

### Display Options:
- : Suppresses verbose output, showing only essential information
- : Controls whether to display detailed backup block information
- : Maximum number of records to display before stopping
- : Counter tracking how many records have been displayed
- : Enables continuous monitoring mode, similar to 'tail -f'
- : Enables display of statistical summary information
- : Shows statistics for each individual record type

### Filter Options:
- : Array of boolean flags for each resource manager type
- : Master flag indicating if resource manager filtering is active
- : Transaction ID to filter by when enabled
- : Flag indicating if transaction ID filtering is active
- : RelFileLocator specifying which relation to filter by
- : Flag for extended filtering options
- : Flag indicating if relation filtering is active
- : Specific block number within a relation to filter by
- : Flag indicating if block-level filtering is active
- : Fork number (main, FSM, VM) for relation filtering
- : Flag to filter by full page writes

### Save Options:
- : Directory path where full page images should be saved

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