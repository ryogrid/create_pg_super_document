# xl_xact_parsed_abort

## Location
src/include/access/xact.h: 404 - 426

## Overview
A structure representing a parsed transaction abort record from PostgreSQL's Write-Ahead Log (WAL), containing all the detailed information extracted from a raw xl_xact_abort record for easier access during WAL replay and analysis.

## Definition


## Detailed Description
The  structure is used to hold the parsed contents of a transaction abort WAL record. Unlike the raw  record format that stores data in a compact binary layout, this structure provides direct access to all components of an abort record in a convenient format.

This structure is populated by the  function, which extracts and parses the various optional components from the raw WAL record based on the info flags present. The parsed structure is then used throughout the system for WAL replay, logical replication, recovery processing, and WAL summarization.

The structure supports various optional components that may be present in an abort record, including subtransactions, dropped relations, dropped statistics, two-phase commit information, and replication origin data.

## Parameters / Member Variables
- : Timestamp when the transaction was aborted
- : Extended information flags indicating which optional components are present
- : Database OID where the transaction occurred (MyDatabaseId)
- : Tablespace OID for the database (MyDatabaseTableSpace)
- : Number of subtransactions that were aborted along with the main transaction
- : Array of subtransaction IDs that were aborted
- : Number of relations that were dropped during this transaction
- : Array of RelFileLocator structures for dropped relations
- : Number of statistics objects that were dropped
- : Array of xl_xact_stats_item structures describing dropped statistics
- : Transaction ID for two-phase commit transactions (only for 2PC)
- : Global transaction identifier string for two-phase commit (only for 2PC)
- : LSN from the replication origin (for logical replication)
- : Timestamp from the replication origin (for logical replication)

## Dependencies
- Functions called/Symbols referenced:
  - xl_xact_stats_item
  - GIDSIZE
- Called from (representative examples):
  - ParseAbortRecord (in src/backend/access/rmgrdesc/xactdesc.c:141)
  - xact_desc_abort (in src/backend/access/rmgrdesc/xactdesc.c:371)
  - xact_redo_abort (in src/backend/access/transam/xact.c:6222)
  - xact_redo (in src/backend/access/transam/xact.c:6334)
  - recoveryStopsBefore (in src/backend/access/transam/xlogrecovery.c:2648)
  - recoveryStopsAfter (in src/backend/access/transam/xlogrecovery.c:2816)
  - SummarizeXactRecord (in src/backend/postmaster/walsummarizer.c:1395)
  - xact_decode (in src/backend/replication/logical/decode.c:249)
  - DecodeAbort (in src/backend/replication/logical/decode.c:851)

## Notes and Other Information
- This structure is defined in src/include/access/xact.h:404-426
- The structure is filled by parsing a raw xl_xact_abort record using ParseAbortRecord()
- Not all fields are always populated - presence depends on the xinfo flags in the original record
- The two-phase commit fields (twophase_xid, twophase_gid) are only meaningful for prepared transactions
- The origin fields (origin_lsn, origin_timestamp) are used for logical replication to track the original source of changes
- The GIDSIZE constant (200) defines the maximum length for global transaction identifiers
- This parsed format makes it easier for various PostgreSQL subsystems to access abort record information without having to repeatedly parse the compact WAL format