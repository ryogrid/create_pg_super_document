# pg_control_checkpoint

## Location
[src/backend/utils/misc/pg_controldata.c:70-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/pg_controldata.c#L70-L162)

## Overview
A PostgreSQL SQL function that retrieves comprehensive checkpoint information from the control file, returning detailed state about the most recent checkpoint operation.

## Definition

```c
Datum
pg_control_checkpoint(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function provides access to the checkpoint information stored in PostgreSQL's control file. It reads the control file under lock protection, extracts detailed checkpoint metadata, and returns it as an 18-field composite tuple. The function includes transaction ID tracking, timeline management, WAL file identification, and various system state indicators that are critical for recovery and replication operations. It calculates the WAL filename containing the checkpoint's REDO start point and formats transaction IDs appropriately for display.

## Parameters / Member Variables
- Returns a composite tuple containing 18 fields:
  - : LSN of the checkpoint record
  - : LSN where REDO should start from
  - : Name of the WAL file containing the REDO start point
  - : Current timeline ID
  - : Previous timeline ID  
  - : Whether full page writes are enabled
  - : Next transaction ID (formatted as epoch:xid)
  - : Next object ID to be assigned
  - : Next multixact ID
  - : Next multixact offset
  - : Oldest transaction ID still visible
  - : Database containing the oldest XID
  - : Oldest active transaction ID
  - : Oldest multixact ID
  - : Database containing oldest multixact
  - : Oldest XID with commit timestamp
  - : Newest XID with commit timestamp
  - : Timestamp when checkpoint was taken

## Dependencies
- Functions called/Symbols referenced:
  - [get_call_result_type](../g/get_call_result_type.md): Validates return type
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages concurrent access to control file
  - [get_controlfile](../g/get_controlfile.md): Reads and parses the control file
  - XLByteToSeg: Converts LSN to WAL segment number
  - [XLogFileName](../X/XLogFileName.md): Generates WAL filename from timeline and segment
  - [LSNGetDatum](../L/LSNGetDatum.md): Converts LSN to PostgreSQL Datum
  - Various converter functions (TransactionIdGetDatum, ObjectIdGetDatum, etc.)
  - EpochFromFullTransactionId/XidFromFullTransactionId: Extract XID components
  - [ControlFileData](../C/ControlFileData.md): Structure containing control file data
- Called from (representative examples):
  - SQL queries via function call mechanism

## Notes and Other Information
- Requires shared lock on ControlFileLock to ensure consistent reads
- Validates control file CRC checksum and raises ERROR if corrupted
- Formats next XID as 'epoch:xid' string for better readability
- Critical for monitoring checkpoint state and recovery planning
- Part of the administrative interface for checkpoint monitoring
- Located in src/backend/utils/misc/pg_controldata.c:70-162

## Simplified Source

```c
Datum
pg_control_checkpoint(PG_FUNCTION_ARGS)
{
    Datum values[18];
    bool nulls[18];
    TupleDesc tupdesc;

    // Validate function return type is composite
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    // Read control file with shared lock
    LWLockAcquire(ControlFileLock, LW_SHARED);
    bool crc_ok;
    ControlFileData *ControlFile = get_controlfile(DataDir, &crc_ok);
    LWLockRelease(ControlFileLock);

    // Verify control file integrity
    if (!crc_ok)
        ereport(ERROR, (errmsg("calculated CRC checksum does not match value stored in file")));

    // Calculate WAL filename for checkpoint REDO point
    XLogSegNo segno;
    char xlogfilename[MAXFNAMELEN];
    XLByteToSeg(ControlFile->checkPointCopy.redo, segno, wal_segment_size);
    XLogFileName(xlogfilename, ControlFile->checkPointCopy.ThisTimeLineID, segno, wal_segment_size);

    // Extract checkpoint information into return values
    values[0] = LSNGetDatum(ControlFile->checkPoint);                    // Checkpoint LSN
    values[1] = LSNGetDatum(ControlFile->checkPointCopy.redo);          // REDO start LSN
    values[2] = CStringGetTextDatum(xlogfilename);                      // WAL filename
    values[3] = Int32GetDatum(ControlFile->checkPointCopy.ThisTimeLineID); // Current timeline
    values[4] = Int32GetDatum(ControlFile->checkPointCopy.PrevTimeLineID); // Previous timeline
    values[5] = BoolGetDatum(ControlFile->checkPointCopy.fullPageWrites);  // Full page writes

    // Format next XID as "epoch:xid" string
    values[6] = CStringGetTextDatum(psprintf("%u:%u",
                EpochFromFullTransactionId(ControlFile->checkPointCopy.nextXid),
                XidFromFullTransactionId(ControlFile->checkPointCopy.nextXid)));

    // Transaction and object tracking
    values[7] = ObjectIdGetDatum(ControlFile->checkPointCopy.nextOid);           // Next OID
    values[8] = TransactionIdGetDatum(ControlFile->checkPointCopy.nextMulti);    // Next multixact
    values[9] = TransactionIdGetDatum(ControlFile->checkPointCopy.nextMultiOffset); // Multixact offset
    values[10] = TransactionIdGetDatum(ControlFile->checkPointCopy.oldestXid);   // Oldest XID
    values[11] = ObjectIdGetDatum(ControlFile->checkPointCopy.oldestXidDB);      // Oldest XID DB
    values[12] = TransactionIdGetDatum(ControlFile->checkPointCopy.oldestActiveXid); // Oldest active XID
    values[13] = TransactionIdGetDatum(ControlFile->checkPointCopy.oldestMulti); // Oldest multixact
    values[14] = ObjectIdGetDatum(ControlFile->checkPointCopy.oldestMultiDB);    // Oldest multixact DB
    values[15] = TransactionIdGetDatum(ControlFile->checkPointCopy.oldestCommitTsXid); // Oldest commit TS XID
    values[16] = TransactionIdGetDatum(ControlFile->checkPointCopy.newestCommitTsXid); // Newest commit TS XID
    values[17] = TimestampTzGetDatum(time_t_to_timestamptz(ControlFile->checkPointCopy.time)); // Checkpoint time

    // All fields are non-null
    memset(nulls, false, sizeof(nulls));

    // Create and return composite tuple
    HeapTuple htup = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
```