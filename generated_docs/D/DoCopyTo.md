# DoCopyTo

## Location
src/backend/commands/copyto.c: 747 - 906

## Overview
DoCopyTo executes the main logic of a COPY TO operation, reading tuples from a relation or query and formatting them for output to the configured destination.

## Definition
```c
uint64 DoCopyTo(CopyToState cstate)
```

## Detailed Description
DoCopyTo performs the core execution of COPY TO operations by coordinating data retrieval, formatting, and output. It handles both relation-based and query-based copying by setting up appropriate scan mechanisms, configures output functions for each column based on binary or text format requirements, and manages a temporary memory context for row processing to prevent memory leaks. The function generates appropriate headers and trailers for binary format, handles CSV headers when requested, and processes each row through either table scanning or query execution. It also provides progress reporting throughout the operation.

## Parameters / Member Variables
- `cstate`: CopyToState structure containing all configuration and state for the copy operation

## Dependencies
- Functions called/Symbols referenced:
  - [SendCopyBegin](../S/SendCopyBegin.md)
  - makeStringInfo
  - [getTypeBinaryOutputInfo](../g/getTypeBinaryOutputInfo.md)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md)
  - [fmgr_info](../f/fmgr_info.md)
  - AllocSetContextCreate
  - [CopySendData](../C/CopySendData.md)
  - [CopySendInt32](../C/CopySendInt32.md)
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - [CopySendChar](../C/CopySendChar.md)
  - [CopyAttributeOutCSV](../C/CopyAttributeOutCSV.md)
  - [CopyAttributeOutText](../C/CopyAttributeOutText.md)
  - [CopySendEndOfRow](../C/CopySendEndOfRow.md)
  - [table_beginscan](../t/table_beginscan.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - slot_getallattrs
  - [CopyOneRowTo](../C/CopyOneRowTo.md)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [table_endscan](../t/table_endscan.md)
  - [ExecutorRun](../E/ExecutorRun.md)
  - [CopySendInt16](../C/CopySendInt16.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [SendCopyEnd](../S/SendCopyEnd.md)
- Called from (representative examples):
  - [DoCopy](DoCopy.md)
  - test_copy_to_callback

## Notes and Other Information
The function handles two distinct data sources: direct table scans for relation-based copies and query execution for complex queries. It manages memory efficiently by creating a temporary row context that gets reset for each row, preventing accumulation of memory during large copy operations. The function supports both binary and text formats, with binary format requiring special signatures and trailers. Progress reporting is integrated throughout the process to provide feedback on large operations. The function returns the total number of processed rows for reporting purposes.