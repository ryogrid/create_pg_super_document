# xl_xact_xinfo

## Location
[src/include/access/xact.h:244-253](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L244-L253)

## Overview
WAL record sub-structure that contains extended information flags for commit and abort transaction records, indicating which optional data chunks are present.

## Definition
```c
typedef struct xl_xact_xinfo
{
    /*
     * Even though we right now only require two bytes of space in xinfo we
     * use four so following records don't have to care about alignment.
     * Commit records can be large, so copying large portions isn't
     * attractive.
     */
    uint32  xinfo;
} xl_xact_xinfo;
```

## Detailed Description
xl_xact_xinfo is a sub-record structure used within commit and abort WAL records to indicate which optional information chunks are included in the record. This design allows PostgreSQL to create minimal commit/abort records that only contain necessary information, rather than always including all possible data.

The structure serves as a header that contains bit flags indicating the presence of various optional data chunks such as database information, subtransaction lists, invalidation messages, dropped statistics, and more. The XLOG_XACT_HAS_INFO bit in the xl_info field of the main record indicates whether this xinfo sub-record is present.

The xinfo field uses a 32-bit integer to provide alignment benefits, even though currently only the lower bits are used for flags. This design choice prevents the need for alignment considerations in subsequent record chunks, which is important since commit records can be quite large and copying large portions during WAL replay would be performance-intensive.

## Parameters / Member Variables
- `xinfo`: 32-bit flag field indicating which optional data chunks follow this header. Possible flags include:
  - XACT_XINFO_HAS_DBINFO (database/tablespace info)
  - XACT_XINFO_HAS_SUBXACTS (subtransaction list)
  - XACT_XINFO_HAS_RELFILELOCATORS (relation file locators)
  - XACT_XINFO_HAS_INVALS (shared invalidation messages)
  - XACT_XINFO_HAS_TWOPHASE (two-phase commit info)
  - XACT_XINFO_HAS_ORIGIN (replication origin info)
  - XACT_XINFO_HAS_AE_LOCKS (acquired exclusive locks)
  - XACT_XINFO_HAS_GID (global transaction ID)
  - XACT_XINFO_HAS_DROPPED_STATS (dropped statistics info)

## Dependencies
- Functions called/Symbols referenced:
  - uint32 (standard type)
- Called from (representative examples):
  - ParseCommitRecord (parses xinfo flags from WAL records)
  - ParseAbortRecord (parses xinfo flags from abort records)
  - XactLogCommitRecord (creates commit records with xinfo)
  - XactLogAbortRecord (creates abort records with xinfo)

## Notes and Other Information
- Located in src/include/access/xact.h:244-253
- Uses 32-bit alignment to avoid alignment issues in subsequent record chunks
- Only present in WAL records when XLOG_XACT_HAS_INFO bit is set in xl_info
- Enables efficient, minimal WAL record formats by including only necessary information
- Critical for WAL parsing and replay functionality during recovery
- The flag-based design allows for future extensibility without breaking existing record formats
- Part of the larger commit/abort record structure hierarchy in PostgreSQL's WAL system