# DoCopyTo

## Location
[src/backend/commands/copyto.c:747-906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L747-L906)

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
  - [makeStringInfo](../m/makeStringInfo.md)
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
  - [slot_getallattrs](../s/slot_getallattrs.md)
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
  - [test_copy_to_callback](../t/test_copy_to_callback.md)

## Notes and Other Information
The function handles two distinct data sources: direct table scans for relation-based copies and query execution for complex queries. It manages memory efficiently by creating a temporary row context that gets reset for each row, preventing accumulation of memory during large copy operations. The function supports both binary and text formats, with binary format requiring special signatures and trailers. Progress reporting is integrated throughout the process to provide feedback on large operations. The function returns the total number of processed rows for reporting purposes.

## Simplified Source

```c
uint64 DoCopyTo(CopyToState cstate) {
    bool pipe = (cstate->filename == NULL && cstate->data_dest_cb == NULL);
    bool fe_copy = (pipe && whereToSendOutput == DestRemote);
    TupleDesc tupDesc;
    uint64 processed;

    // Start frontend copy protocol if needed
    if (fe_copy) {
        SendCopyBegin(cstate);
    }

    // Get tuple descriptor from relation or query
    if (cstate->rel) {
        tupDesc = RelationGetDescr(cstate->rel);
    } else {
        tupDesc = cstate->queryDesc->tupDesc;
    }

    // Set up output buffer and functions
    cstate->fe_msgbuf = makeStringInfo();
    setup_output_functions(cstate, tupDesc);

    // Create temporary memory context for row processing
    cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
                                               "COPY TO", ALLOCSET_DEFAULT_SIZES);

    // Handle binary or text format headers
    if (cstate->opts.binary) {
        // Send binary format signature and headers
        CopySendData(cstate, BinarySignature, 11);
        CopySendInt32(cstate, 0); // flags
        CopySendInt32(cstate, 0); // header extension
    } else {
        // Handle encoding conversion for text format
        if (cstate->need_transcoding) {
            cstate->opts.null_print_client = pg_server_to_any(
                cstate->opts.null_print, cstate->opts.null_print_len,
                cstate->file_encoding);
        }

        // Send CSV header line if requested
        if (cstate->opts.header_line) {
            send_csv_header(cstate, tupDesc);
        }
    }

    // Process data based on source type
    if (cstate->rel) {
        // Direct table scan
        TableScanDesc scandesc;
        TupleTableSlot *slot;

        scandesc = table_beginscan(cstate->rel, GetActiveSnapshot(), 0, NULL);
        slot = table_slot_create(cstate->rel, NULL);

        processed = 0;
        while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot)) {
            CHECK_FOR_INTERRUPTS();

            // Get all attribute values from the slot
            slot_getallattrs(slot);

            // Format and send the row data
            CopyOneRowTo(cstate, slot);

            // Update progress reporting
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, ++processed);
        }

        // Cleanup table scan
        ExecDropSingleTupleTableSlot(slot);
        table_endscan(scandesc);
    } else {
        // Query execution - let the dest receiver handle tuples
        ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0, true);
        processed = ((DR_copy *) cstate->queryDesc->dest)->processed;
    }

    // Send binary format trailer if needed
    if (cstate->opts.binary) {
        CopySendInt16(cstate, -1);  // end marker
        CopySendEndOfRow(cstate);   // flush trailer
    }

    // Cleanup memory context
    MemoryContextDelete(cstate->rowcontext);

    // End frontend copy protocol if needed
    if (fe_copy) {
        SendCopyEnd(cstate);
    }

    return processed;
}
```