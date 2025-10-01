# WriteMTruncateXlogRec

## Location
[src/backend/access/transam/multixact.c:3361-3385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3361-L3385)

## Overview
WriteMTruncateXlogRec is a static function that writes a TRUNCATE xlog record for multixact operations, ensuring the record is flushed to disk before returning.

## Definition

```c
static void
WriteMTruncateXlogRec(Oid oldestMultiDB,
					  MultiXactId startTruncOff, MultiXactId endTruncOff,
					  MultiXactOffset startTruncMemb, MultiXactOffset endTruncMemb)
```
## Detailed Description
This function creates and writes a TRUNCATE xlog record (xl_multixact_truncate) for multixact operations. It populates the record with the provided parameters and ensures the xlog record is flushed to disk before returning, which is critical for consistency similar to TruncateCLOG() operations. The function uses the standard xlog insertion pattern: begin insert, register data, insert record, and flush.

## Parameters / Member Variables
- : The oldest multixact database OID
- : Starting MultiXactId for the truncation range of offsets
- : Ending MultiXactId for the truncation range of offsets  
- : Starting MultiXactOffset for the truncation range of members
- : Ending MultiXactOffset for the truncation range of members

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogFlush](../X/XLogFlush.md)
  - [xl_multixact_truncate](../x/xl_multixact_truncate.md) (struct)
  - SizeOfMultiXactTruncate
  - XLOG_MULTIXACT_TRUNCATE_ID
- Called from:
  - [TruncateMultiXact](../T/TruncateMultiXact.md)
  - debug_elog6

## Notes and Other Information
- The function must flush the xlog record to disk before returning for consistency requirements
- Uses the MULTIXACT resource manager (RM_MULTIXACT_ID) for xlog operations
- Part of the multixact subsystem that manages multiple transaction IDs sharing locks
- Located in src/backend/access/transam/multixact.c:3361-3385

## Simplified Source

```c
static void
WriteMTruncateXlogRec(Oid oldestMultiDB,
                      MultiXactId startTruncOff, MultiXactId endTruncOff,
                      MultiXactOffset startTruncMemb, MultiXactOffset endTruncMemb)
{
    XLogRecPtr recptr;
    xl_multixact_truncate xlrec;

    // Prepare the multixact truncation record
    xlrec.oldestMultiDB = oldestMultiDB;
    xlrec.startTruncOff = startTruncOff;
    xlrec.endTruncOff = endTruncOff;
    xlrec.startTruncMemb = startTruncMemb;
    xlrec.endTruncMemb = endTruncMemb;

    // Write and flush the WAL record
    XLogBeginInsert();
    XLogRegisterData((char *) (&xlrec), SizeOfMultiXactTruncate);
    recptr = XLogInsert(RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID);
    XLogFlush(recptr);  // Must flush for consistency
}
```