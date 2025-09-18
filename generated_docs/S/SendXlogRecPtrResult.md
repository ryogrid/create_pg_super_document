# SendXlogRecPtrResult

## Location
src/backend/backup/basebackup_copy.c: 341 - 377

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
  - CreateDestReceiver
  - DestRemoteSimple
  - CreateTemplateTupleDesc
  - TupleDescInitBuiltinEntry
  - begin_tup_output_tupdesc
  - CStringGetTextDatum
  - Int64GetDatum
  - do_tup_output
  - end_tup_output
  - pq_puttextmessage
  - PqMsg_CommandComplete
- Called from (representative examples):
  - bbsink_copystream_begin_backup
  - bbsink_copystream_end_backup

## Notes and Other Information
- This is a static function limited to the basebackup_copy.c file
- Uses INT8OID for TimeLineID despite it being unsigned, as INT4 would not be wide enough
- The XLogRecPtr is formatted as a hexadecimal string in the format 'X/X' using LSN_FORMAT_ARGS
- Sends a complete result set with proper PostgreSQL protocol messages including RowDescription and CommandComplete
- Used to communicate WAL position information during base backup operations