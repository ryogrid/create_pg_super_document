# XLogInsert

## Location
[src/backend/access/transam/xloginsert.c:474-547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L474-L547)

## Overview
XLogInsert is the primary function that finalizes and inserts a constructed WAL record into the Write-Ahead Log, returning the LSN for the inserted record.

## Definition
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)

## Detailed Description
XLogInsert is the culminating function in PostgreSQL's WAL record construction and insertion process. It takes all the data, buffer references, and flags registered through previous XLogRegister* calls and creates a complete WAL record with the specified resource manager ID (rmid) and info byte.

The function performs several critical operations: validates that XLogBeginInsert() was called, checks info byte validity, handles bootstrap mode specially, determines whether full-page writes are needed, assembles the complete record using XLogRecordAssemble(), and finally inserts it via XLogInsertRecord(). The process may retry if insertion fails due to timing issues with full-page write requirements.

The function implements the fundamental WAL principle "write the log before the data" by returning an LSN that represents the point up to which WAL must be flushed before any associated data pages can be written to disk. This LSN serves as a durability guarantee for the logged operation.

After successful insertion, the function cleans up all registration state via XLogResetInsertion(), preparing for the next WAL record construction cycle.

## Parameters / Member Variables
- rmid: Resource Manager ID identifying which subsystem owns this record type (e.g., RM_HEAP_ID for heap operations, RM_BTREE_ID for B-tree operations)
- info: 8-bit info field containing operation-specific flags and information, with certain bits reserved for system use

## Dependencies
- Functions called/Symbols referenced:
  - [GetFullPageWriteInfo](../G/GetFullPageWriteInfo.md) (determines full-page write requirements)
  - [XLogRecordAssemble](XLogRecordAssemble.md) (assembles the complete WAL record)
  - [XLogInsertRecord](XLogInsertRecord.md) (physically inserts the record into WAL)
  - [XLogResetInsertion](XLogResetInsertion.md) (cleans up insertion state)
  - IsBootstrapProcessingMode (checks for bootstrap mode)
  - RmgrId, XLogRecData, XLogRecPtr (data types)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (heap tuple insertions)
  - [_bt_insertonpg](../b/_bt_insertonpg.md) (B-tree insertions)
  - [XactLogCommitRecord](XactLogCommitRecord.md) (transaction commits)
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (checkpoint operations)
  - [log_newpage](../l/log_newpage.md) (new page logging)

## Notes and Other Information
- Must be called after XLogBeginInsert() and all XLogRegister* calls
- Returns InvalidXLogRecPtr on failure, requiring retry
- In bootstrap mode, returns a dummy LSN for non-XLOG resource managers
- The returned LSN can be used to set page LSNs for affected data pages
- Handles full-page write decisions dynamically based on current WAL state
- Central function that coordinates the final WAL record insertion process
- Automatically retries insertion if conditions change during assembly

## Simplified Source

```c
// Simplified version of XLogInsert
XLogRecPtr XLogInsert(RmgrId rmid, uint8 info) {
    XLogRecPtr EndPos;

    // Step 1: Validate that XLogBeginInsert() was called
    if (!begininsert_called)
        elog(ERROR, "XLogBeginInsert was not called");

    // Step 2: Validate info byte contains only allowed flags
    if ((info & ~(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE | XLR_CHECK_CONSISTENCY)) != 0)
        elog(PANIC, "invalid xlog info mask %02X", info);

    // Step 3: Handle bootstrap mode - return dummy LSN for non-XLOG records
    if (IsBootstrapProcessingMode() && rmid != RM_XLOG_ID) {
        XLogResetInsertion();
        return SizeOfXLogLongPHD; // start of 1st checkpoint record
    }

    // Step 4: Main insertion loop - retry until successful
    do {
        XLogRecPtr RedoRecPtr;
        bool doPageWrites;
        bool topxid_included = false;
        XLogRecPtr fpw_lsn;
        XLogRecData *record_data;
        int num_full_page_images = 0;

        // Get current full-page write requirements
        GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

        // Assemble the complete WAL record from registered data
        record_data = XLogRecordAssemble(rmid, info, RedoRecPtr, doPageWrites,
                                       &fpw_lsn, &num_full_page_images, &topxid_included);

        // Insert the assembled record into WAL
        EndPos = XLogInsertRecord(record_data, fpw_lsn, curinsert_flags,
                                num_full_page_images, topxid_included);

    } while (EndPos == InvalidXLogRecPtr); // Retry if insertion failed

    // Step 5: Clean up insertion state for next record
    XLogResetInsertion();

    return EndPos; // Return LSN of inserted record
}
```

Key simplifications made:
- Removed TRACE_POSTGRESQL_WAL_INSERT tracing call for clarity
- Simplified variable names for better readability (e.g., `num_fpi` → `num_full_page_images`)
- Added step-by-step comments explaining the main logic flow
- Consolidated complex validation logic with descriptive comments
- Focused on the main execution path while preserving all essential functionality
- Maintained the retry loop structure which is critical for correctness