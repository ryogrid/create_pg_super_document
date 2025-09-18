# XLogRecordMatchesRelationBlock

## Location
src/bin/pg_waldump/pg_waldump.c: 438 - 468

## Overview
XLogRecordMatchesRelationBlock determines whether a given WAL record matches specific relation and block filtering criteria used in pg_waldump.

## Definition


## Detailed Description
This function iterates through all block references in a WAL record to determine if any of them match the specified filtering criteria. It supports flexible matching where any combination of relation, fork, and block number can be specified or left as wildcards. The function is used by pg_waldump to implement filtering functionality, allowing users to focus on WAL records that affect specific database objects or blocks. It handles empty/invalid values as wildcards, enabling partial matching scenarios.

## Parameters / Member Variables
- : XLogReaderState containing the decoded WAL record to examine
- : RelFileLocator to match against, or emptyRelFileLocator for wildcard matching
- : Specific block number to match, or InvalidBlockNumber for any block
- : Fork number to match, or InvalidForkNumber for any fork

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecMaxBlockId
  - XLogRecGetBlockTagExtended
  - RelFileLocatorEquals
  - InvalidForkNumber (constant)
  - emptyRelFileLocator (constant)
  - InvalidBlockNumber (constant)
- Called from (representative examples):
  - main (used for filtering WAL records in pg_waldump)

## Notes and Other Information
- Returns true if any block reference in the record matches the filtering criteria
- Supports wildcard matching using invalid/empty values for flexible filtering
- Iterates through all block IDs present in the WAL record (0 to XLogRecMaxBlockId)
- Used primarily for implementing the --relation and --block options in pg_waldump
- Enables focused analysis of WAL activity affecting specific database objects