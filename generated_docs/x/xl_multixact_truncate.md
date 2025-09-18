# xl_multixact_truncate

## Location
src/include/access/multixact.h: 83 - 94

## Overview  
xl_multixact_truncate is a WAL record structure used to log the truncation of old multi-transaction data files, enabling cleanup of obsolete multi-transaction information.

## Definition
typedef struct xl_multixact_truncate
{
    Oid         oldestMultiDB;

    /* to-be-truncated range of multixact offsets */
    MultiXactId startTruncOff;  /* just for completeness' sake */
    MultiXactId endTruncOff;

    /* to-be-truncated range of multixact members */
    MultiXactOffset startTruncMemb;
    MultiXactOffset endTruncMemb;
} xl_multixact_truncate;

## Detailed Description
xl_multixact_truncate is a WAL record structure that captures information about multi-transaction file truncation operations. As PostgreSQL runs, old multi-transaction data accumulates in the pg_multixact directory. To prevent unbounded growth, the system periodically truncates old, no-longer-needed multi-transaction files.

This structure records the ranges of both offset and member data that are being truncated. The offset data tracks which MultiXactIds map to which positions in the members file, while the member data contains the actual transaction participants and their lock modes. Both need to be truncated in a coordinated fashion to maintain consistency.

The WAL record ensures that truncation operations can be properly replayed during recovery, maintaining consistency between the primary server and any replicas.

## Parameters / Member Variables
- : The OID of the database that contains the oldest multi-transaction that must be preserved (determines truncation boundary)
- : The starting MultiXactId in the range being truncated from the offsets file (included for completeness but may not be strictly necessary for recovery)
- : The ending MultiXactId in the range being truncated from the offsets file (exclusive boundary)
- : The starting MultiXactOffset in the range being truncated from the members file
- : The ending MultiXactOffset in the range being truncated from the members file (exclusive boundary)

## Dependencies
- Functions called/Symbols referenced:
  - Oid: PostgreSQL object identifier type
  - MultiXactId: Multi-transaction identifier type
  - MultiXactOffset: Offset type for the members file
- Called from (representative examples):
  - WriteMTruncateXlogRec: Creates WAL records using this structure during multi-transaction cleanup operations
  - multixact_redo: Processes xl_multixact_truncate records during recovery to replay truncation operations
  - multixact_desc: Uses this structure for debugging and logging WAL record information

## Notes and Other Information
- This is part of PostgreSQL's automatic cleanup mechanism to prevent pg_multixact directory from growing unboundedly
- Truncation operations are coordinated with vacuum and other maintenance operations to ensure no active multi-transactions are accidentally removed
- The structure records ranges for both offsets and members because these are stored in separate files that must be truncated consistently
- Used with XLOG_MULTIXACT_TRUNCATE_ID WAL record type
- Critical for long-running PostgreSQL installations where multi-transaction usage could otherwise consume excessive disk space
- The SizeOfMultiXactTruncate macro provides the size of this structure for WAL operations
- Ensures that replica servers maintain the same truncated state as the primary server