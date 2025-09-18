# SendXlogRecPtrResult

## Location
[src/backend/backup/basebackup_copy.c:341-377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L341-L377)

## Overview
SendXlogRecPtrResult is a static function that sends a result set containing a single XLogRecPtr record and TimeLineID in text format as part of PostgreSQL base backup operations.

## Definition
```c
static void SendXlogRecPtrResult(XLogRecPtr ptr, TimeLineID tli)
```

## Detailed Description
This function creates and sends a two-column result set containing an XLog record pointer and timeline ID. It constructs a temporary tuple descriptor with 'recptr' (TEXT) and 'tli' (INT8) columns, formats the XLogRecPtr as a hexadecimal string using the LSN_FORMAT_ARGS macro, and sends the data as a single row result set. The function handles the complete protocol sequence including RowDescription, data row, and CommandComplete messages.

## Parameters / Member Variables
- `ptr`: XLogRecPtr - The transaction log record pointer to be sent
- `tli`: TimeLineID - The timeline identifier associated with the XLog pointer

## Dependencies
- Functions called/Symbols referenced:
  - [CreateDestReceiver](../C/CreateDestReceiver.md)
  - DestRemoteSimple
  - [CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)
  - [TupleDescInitBuiltinEntry](../T/TupleDescInitBuiltinEntry.md)
  - [begin_tup_output_tupdesc](../b/begin_tup_output_tupdesc.md)
  - CStringGetTextDatum
  - [Int64GetDatum](../I/Int64GetDatum.md)
  - [do_tup_output](../d/do_tup_output.md)
  - [end_tup_output](../e/end_tup_output.md)
  - [pq_puttextmessage](../p/pq_puttextmessage.md)
  - PqMsg_CommandComplete
- Called from (representative examples):
  - [bbsink_copystream_begin_backup](../b/bbsink_copystream_begin_backup.md)
  - [bbsink_copystream_end_backup](../b/bbsink_copystream_end_backup.md)

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- Uses INT8OID for TimeLineID despite it being unsigned, as INT4 would not be wide enough
- The XLogRecPtr is formatted as a hexadecimal string in the format 'X/X' using LSN_FORMAT_ARGS
- Sends a complete result set with proper PostgreSQL protocol messages including RowDescription and CommandComplete
- Used to communicate WAL position information during base backup operations