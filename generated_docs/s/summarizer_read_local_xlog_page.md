# summarizer_read_local_xlog_page

## Location
[src/backend/postmaster/walsummarizer.c:1497-1610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L1497-L1610)

## Overview
A specialized function that reads WAL (Write-Ahead Log) pages from a specific timeline with built-in waiting capability and timeline transition handling for the WAL summarizer process.

## Definition
```c
static int summarizer_read_local_xlog_page(XLogReaderState *state,
                                          XLogRecPtr targetPagePtr, int reqLen,
                                          XLogRecPtr targetRecPtr, char *cur_page)
```

## Detailed Description
This function serves as a timeline-aware WAL page reader specifically designed for the WAL summarizer process. Unlike the standard `read_local_xlog_page`, it is restricted to reading from one particular timeline and handles both current and historic timelines differently. When reading from the current timeline and reaching the end of available WAL, it waits for more data to become available. For historic timelines, it marks the end of WAL and gives up when no more data is available.

The function maintains state through private data that tracks the timeline ID, read position limits, and whether the timeline is historic. It automatically updates this state when timeline transitions occur, ensuring consistent behavior across timeline boundaries.

## Parameters / Member Variables
- `state`: XLogReaderState structure containing the reader's current state and configuration
- `targetPagePtr`: LSN (Log Sequence Number) of the page to be read
- `reqLen`: Minimum number of bytes required for the read operation
- `targetRecPtr`: LSN of the target record (used for validation)
- `cur_page`: Buffer to store the read page data

## Dependencies
- Functions called/Symbols referenced:
  - [HandleWalSummarizerInterrupts](../H/HandleWalSummarizerInterrupts.md)
  - [summarizer_wait_for_wal](summarizer_wait_for_wal.md)
  - [GetLatestLSN](../G/GetLatestLSN.md)
  - [readTimeLineHistory](../r/readTimeLineHistory.md)
  - [tliSwitchPoint](../t/tliSwitchPoint.md)
  - [WALRead](../W/WALRead.md)
  - [WALReadRaiseError](../W/WALReadRaiseError.md)
- Called from (representative examples):
  - [SummarizeWAL](../S/SummarizeWAL.md)

## Notes and Other Information
- The function expects the caller to properly initialize private_data with timeline ID (tli), read limit (read_upto), and historic status
- For current timelines, the function will wait and retry when insufficient data is available
- Timeline transitions are automatically detected and handled by switching the timeline to historic status
- Page read tracking is maintained via `pages_read_since_last_sleep` for performance optimization
- Returns the number of valid bytes read or -1 when end of WAL is reached on historic timelines
- Uses XLOG_BLCKSZ as the standard block size for WAL page operations

## Simplified Source

```c
static int summarizer_read_local_xlog_page(XLogReaderState *state,
                                          XLogRecPtr targetPagePtr, int reqLen,
                                          XLogRecPtr targetRecPtr, char *cur_page)
{
    int count;
    WALReadError errinfo;
    SummarizerReadLocalXLogPrivate *private_data =
        (SummarizerReadLocalXLogPrivate *) state->private_data;

    HandleWalSummarizerInterrupts();

    while (1) {
        if (targetPagePtr + XLOG_BLCKSZ <= private_data->read_upto) {
            // Full block available
            count = XLOG_BLCKSZ;
            break;
        }
        else if (targetPagePtr + reqLen > private_data->read_upto) {
            // Insufficient data available
            if (private_data->historic) {
                // Historic timeline - no more data will arrive
                private_data->end_of_wal = true;
                return -1;
            }
            else {
                // Current timeline - wait for more data
                HandleWalSummarizerInterrupts();
                summarizer_wait_for_wal();

                // Check if timeline changed or more data arrived
                XLogRecPtr latest_lsn = GetLatestLSN(&latest_tli);
                if (private_data->tli == latest_tli) {
                    // Still current timeline, update read limit
                    private_data->read_upto = latest_lsn;
                }
                else {
                    // Timeline became historic, find switch point
                    List *tles = readTimeLineHistory(latest_tli);
                    XLogRecPtr switchpoint = tliSwitchPoint(private_data->tli, tles, NULL);

                    private_data->historic = true;
                    private_data->read_upto = switchpoint;
                }
            }
        }
        else {
            // Partial block available
            count = private_data->read_upto - targetPagePtr;
            break;
        }
    }

    // Read the WAL page
    if (!WALRead(state, cur_page, targetPagePtr, count, private_data->tli, &errinfo)) {
        WALReadRaiseError(&errinfo);
    }

    ++pages_read_since_last_sleep;
    return count;
}
```