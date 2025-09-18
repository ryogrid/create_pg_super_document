# pg_walfile_name_offset

## Location
src/backend/access/transam/xlogfuncs.c: 373 - 436

## Overview
Computes the WAL file name and decimal byte offset for a given WAL location, returning both values as a tuple.

## Definition
```c
Datum pg_walfile_name_offset(PG_FUNCTION_ARGS)
```

## Detailed Description
This function takes a WAL (Write-Ahead Log) location (LSN) as input and computes the corresponding WAL file name and byte offset within that file. This is useful for determining which specific WAL file contains a particular WAL record and where within that file the record is located. The function returns a tuple containing both the file name (as text) and the offset (as integer).

The function ensures that it cannot be executed during recovery mode, similar to other WAL control functions. It constructs a tuple descriptor to return structured data with two fields: `file_name` and `file_offset`.

## Parameters / Member Variables
- `locationpoint`: The input LSN (Log Sequence Number) obtained via `PG_GETARG_LSN(0)`
- `xlogsegno`: Calculated WAL segment number
- `xrecoff`: Calculated byte offset within the WAL segment
- `xlogfilename`: Buffer to store the computed WAL filename
- `values[2]`: Array to store the return values (filename and offset)
- `isnull[2]`: Array to indicate null status for return values

## Dependencies
- Functions called/Symbols referenced:
  - `[RecoveryInProgress](../R/RecoveryInProgress.md)()` - Checks if database recovery is currently active
  - `[CreateTemplateTupleDesc](../C/CreateTemplateTupleDesc.md)()` - Creates tuple descriptor for result
  - `[TupleDescInitEntry](../T/TupleDescInitEntry.md)()` - Initializes tuple descriptor entries
  - `[BlessTupleDesc](../B/BlessTupleDesc.md)()` - Finalizes tuple descriptor
  - `XLByteToSeg()` - Converts LSN to segment number
  - `[GetWALInsertionTimeLine](../G/GetWALInsertionTimeLine.md)()` - Gets current WAL insertion timeline
  - `[XLogFileName](../X/XLogFileName.md)()` - Generates WAL filename from segment info
  - `XLogSegmentOffset()` - Calculates offset within segment
  - `CStringGetTextDatum()`, `UInt32GetDatum()` - Convert values to Datums
  - `[heap_form_tuple](../h/heap_form_tuple.md)()`, `HeapTupleGetDatum()` - Create and return tuple
  - `PG_RETURN_DATUM` - Macro to return Datum value
- Called from (representative examples):
  - No direct callers found in the codebase (SQL-callable function)

## Notes and Other Information
- The function will raise an error if called during recovery, with error code `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`
- Returns a composite type (tuple) with two fields: `file_name` (text) and `file_offset` (int4)
- The computed filename follows PostgreSQL's standard WAL filename format
- Useful for debugging and administrative tasks involving specific WAL locations
- The function is accessible via SQL as a system function
- Located in `src/backend/access/transam/xlogfuncs.c:373-436`
- Input LSN can come from functions like `pg_backup_stop()` or `pg_switch_wal()`
- The offset represents the byte position within the specific WAL segment file