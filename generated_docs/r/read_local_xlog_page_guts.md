# read_local_xlog_page_guts

## Location
[src/backend/access/transam/xlogutils.c:885-1019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L885-L1019)

## Overview
Core implementation function that reads WAL (Write-Ahead Log) pages from local storage, handling both waiting and non-waiting variants for WAL data availability.

## Definition
static int read_local_xlog_page_guts(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen, XLogRecPtr targetRecPtr, char *cur_page, bool wait_for_wal)

## Detailed Description
This internal function serves as the backbone for reading WAL pages locally, implementing sophisticated logic to handle different database states (normal operation vs recovery) and timeline management. The function operates in a loop that waits for WAL data to become available when necessary, determining the appropriate read limits based on the current system state.

Key behaviors include:
- Determining read limits based on whether the system is in recovery (using GetXLogReplayRecPtr) or normal operation (using GetFlushRecPtr)
- Handling timeline switches in cascading standby configurations
- Supporting non-blocking reads when wait_for_wal is false
- Managing historical timeline reads with appropriate limits
- Calculating optimal read sizes (full XLOG_BLCKSZ blocks when possible)

## Parameters / Member Variables
- : XLogReaderState containing the reader's current state and configuration
- : XLogRecPtr specifying the WAL position where the target page begins
- : Integer indicating the minimum number of bytes required to be read
- : XLogRecPtr of the target WAL record being sought
- : Character buffer where the read WAL page data will be stored
- : Boolean flag controlling whether to wait for WAL data availability or return immediately

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [GetFlushRecPtr](../G/GetFlushRecPtr.md)
  - [GetXLogReplayRecPtr](../G/GetXLogReplayRecPtr.md)
  - [XLogReadDetermineTimeline](../X/XLogReadDetermineTimeline.md)
  - [pg_usleep](../p/pg_usleep.md)
  - [WALRead](../W/WALRead.md)
  - [WALReadRaiseError](../W/WALReadRaiseError.md)
- Called from (representative examples):
  - [read_local_xlog_page](read_local_xlog_page.md)
  - [read_local_xlog_page_no_wait](read_local_xlog_page_no_wait.md)

## Notes and Other Information
- The function implements a sophisticated waiting loop that checks for interrupts and sleeps briefly when WAL data is not yet available
- Timeline handling is particularly complex in cascading standby scenarios where the current timeline might become historical during operation
- The function includes detailed comments explaining edge cases in cascading standby configurations
- Returns -1 when insufficient data is available and wait_for_wal is false
- Uses WALReadRaiseError to handle read errors, ensuring proper error propagation
- File location: src/backend/access/transam/xlogutils.c:885-1019

## Simplified Source

```c
static int read_local_xlog_page_guts(XLogReaderState *state, XLogRecPtr targetPagePtr,
                                    int reqLen, XLogRecPtr targetRecPtr,
                                    char *cur_page, bool wait_for_wal)
{
    XLogRecPtr read_upto, loc;
    TimeLineID tli, currTLI;
    int count;
    WALReadError errinfo;

    loc = targetPagePtr + reqLen;

    // Main loop: wait for WAL data to become available
    while (1) {
        // Determine how much WAL we can currently read
        if (!RecoveryInProgress())
            read_upto = GetFlushRecPtr(&currTLI);  // Normal operation
        else
            read_upto = GetXLogReplayRecPtr(&currTLI);  // Recovery mode

        tli = currTLI;

        // Handle timeline changes (important for cascading standbys)
        XLogReadDetermineTimeline(state, targetPagePtr, reqLen, tli);

        if (state->currTLI == currTLI) {
            // We're on the current timeline
            if (loc <= read_upto)
                break;  // Data is available

            // Not enough data yet
            if (!wait_for_wal) {
                // Mark end of WAL reached for no-wait callers
                ReadLocalXLogPageNoWaitPrivate *private_data =
                    (ReadLocalXLogPageNoWaitPrivate *) state->private_data;
                private_data->end_of_wal = true;
                break;
            }

            // Wait a bit and check for interrupts
            CHECK_FOR_INTERRUPTS();
            pg_usleep(1000L);
        } else {
            // We're on a historical timeline - limit to switch point
            read_upto = state->currTLIValidUntil;
            tli = state->currTLI;
            break;  // No need to wait for historical data
        }
    }

    // Calculate how much to read
    if (targetPagePtr + XLOG_BLCKSZ <= read_upto) {
        count = XLOG_BLCKSZ;  // Read full block
    } else if (targetPagePtr + reqLen > read_upto) {
        return -1;  // Not enough data available
    } else {
        count = read_upto - targetPagePtr;  // Read what's available
    }

    // Perform the actual WAL read
    if (!WALRead(state, cur_page, targetPagePtr, count, tli, &errinfo))
        WALReadRaiseError(&errinfo);

    return count;  // Return number of bytes read
}
```