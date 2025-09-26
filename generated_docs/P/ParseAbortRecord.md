# ParseAbortRecord

## Location
[src/backend/access/rmgrdesc/xactdesc.c:141-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/rmgrdesc/xactdesc.c#L141-L238)

## Overview
ParseAbortRecord parses the WAL format of a transaction abort record and converts it into an easier-to-understand structured format for use by both backend and frontend code.

## Definition
```c
void ParseAbortRecord(uint8 info, xl_xact_abort *xlrec, xl_xact_parsed_abort *parsed)
```

## Detailed Description
This function is the abort counterpart to ParseCommitRecord, responsible for parsing binary WAL abort record data into a structured format. It extracts similar transaction metadata including timing information, database/tablespace identifiers, subtransaction information, relation file locators, dropped statistics, and two-phase commit data. Like ParseCommitRecord, it handles variable-length records by sequentially parsing optional sections based on info flags, but is specifically designed for transaction abort scenarios. The function is essential for WAL replay during recovery and for frontend tools analyzing aborted transactions.

## Parameters / Member Variables
- `info`: Info flags indicating which optional sections are present in the WAL record
- `xlrec`: Pointer to the raw WAL abort record data structure
- `parsed`: Output structure to store the parsed abort record information

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - [strlcpy](../s/strlcpy.md)
  - memcpy
  - strlen
  - MinSizeOfXactAbort
  - XLOG_XACT_HAS_INFO
  - XACT_XINFO_HAS_DBINFO
  - XACT_XINFO_HAS_SUBXACTS
  - XACT_XINFO_HAS_RELFILELOCATORS
  - XACT_XINFO_HAS_DROPPED_STATS
  - XACT_XINFO_HAS_TWOPHASE
  - XACT_XINFO_HAS_GID
  - XACT_XINFO_HAS_ORIGIN
- Called from (representative examples):
  - [xact_desc_abort](../x/xact_desc_abort.md)
  - [xact_redo](../x/xact_redo.md)
  - [recoveryStopsBefore](../r/recoveryStopsBefore.md)
  - [recoveryStopsAfter](../r/recoveryStopsAfter.md)
  - [SummarizeXactRecord](../S/SummarizeXactRecord.md)
  - [xact_decode](../x/xact_decode.md)

## Notes and Other Information
- Structurally similar to ParseCommitRecord but handles abort-specific data
- Located in xactdesc.c to allow sharing between backend and frontend code
- Handles variable-length WAL records with optional sections
- No alignment is guaranteed after the XACT_XINFO_HAS_TWOPHASE section
- Critical for WAL replay, recovery, replication, and analysis of aborted transactions
- Note that abort records don't include invalidation messages (XACT_XINFO_HAS_INVALS) unlike commit records