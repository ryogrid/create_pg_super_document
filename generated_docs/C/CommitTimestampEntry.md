# CommitTimestampEntry

## Location
[src/backend/access/transam/commit_ts.c:54-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/commit_ts.c#L54-L58)

## Overview
CommitTimestampEntry is a data structure that stores commit timestamp information for transactions, containing both the commit time and the replication origin node identifier.

## Definition


## Detailed Description
CommitTimestampEntry is a fundamental data structure in PostgreSQL's commit timestamp tracking system. Each entry occupies 8+2 bytes (10 bytes total) and represents the commit information for a single transaction. The structure is designed to be compact to optimize storage efficiency in the commit timestamp SLRU (Simple Least Recently Used) buffer system.

This structure is part of PostgreSQL's commit timestamp feature, which allows tracking when transactions were committed and from which replication origin they originated. This information is crucial for logical replication, conflict resolution, and transaction ordering in distributed PostgreSQL setups.

The size constraint (8+2 bytes) is mentioned in the source comments as being important for file naming in the SLRU system, where enlarging this struct could affect the maximum possible file name length in SlruScanDirectory operations.

## Parameters / Member Variables
- : A TimestampTz value representing the exact timestamp when the transaction was committed, stored with timezone information
- : A RepOriginId identifying the replication origin node from which this transaction originated, used in logical replication scenarios

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId (type)
  - TimestampTz (type)
- Called from (representative examples):
  - SizeOfCommitTimestampEntry (macro/constant definition)
  - [CommitTimestampShared](CommitTimestampShared.md) (struct member)
  - [TransactionIdSetCommitTs](../T/TransactionIdSetCommitTs.md) (function)
  - [TransactionIdGetCommitTsData](../T/TransactionIdGetCommitTsData.md) (function)

## Notes and Other Information
- The structure size is carefully designed to be exactly 10 bytes to optimize storage in PostgreSQL's SLRU buffer system
- This structure is central to PostgreSQL's commit timestamp tracking feature, which can be enabled/disabled via the track_commit_timestamp configuration parameter
- The nodeid field is particularly important in logical replication setups where transactions may originate from different nodes
- The compact size helps minimize I/O overhead when reading/writing commit timestamp data to disk
- File naming constraints in the SLRU system depend on keeping this structure size reasonable