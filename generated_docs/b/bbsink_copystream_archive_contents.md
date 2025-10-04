# bbsink_copystream_archive_contents

## Location
[src/backend/backup/basebackup_copy.c:183-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_copy.c#L183-L240)

## Overview
Sends a CopyData message containing a chunk of archive content and manages progress reporting to the client during backup transmission.

## Definition
static void bbsink_copystream_archive_contents(bbsink *sink, size_t len)

## Detailed Description
This function handles the transmission of archive content chunks during a basebackup operation. It sends the archive data to the client via CopyData messages (if send_to_client is enabled) and implements intelligent progress reporting to avoid overwhelming the client with status updates. The progress reporting mechanism only checks system time after a certain number of bytes have been transmitted (PROGRESS_REPORT_BYTE_INTERVAL), and only sends progress messages when sufficient time has elapsed (PROGRESS_REPORT_MILLISECOND_THRESHOLD) or if the system clock moved backward. Each CopyData message includes a 'd' type byte prefix to identify it as archive/manifest data.

## Parameters / Member Variables
- : Pointer to the base bbsink structure (cast to bbsink_copystream internally)
- : Size in bytes of the archive content chunk to be sent

## Dependencies
- Functions called/Symbols referenced:
  - pq_putmessage
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md)
  - [pq_beginmessage](../p/pq_beginmessage.md)
  - [pq_sendbyte](../p/pq_sendbyte.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_endmessage](../p/pq_endmessage.md)
  - pq_flush_if_writable
  - PROGRESS_REPORT_BYTE_INTERVAL (65536 bytes)
  - PROGRESS_REPORT_MILLISECOND_THRESHOLD (1000 ms)
  - PqMsg_CopyData
- Called from (representative examples):
  - Referenced by bbsink_copystream_ops structure as the archive_contents handler

## Notes and Other Information
- Progress reporting uses a two-stage throttling mechanism: byte threshold (65536) and time threshold (1000ms)
- Handles system clock changes gracefully by sending progress reports if time moves backward
- The message buffer includes the 'd' type byte plus the actual content (len + 1 total bytes)
- Only sends data to client when send_to_client flag is true, allowing for other destination types
- Progress reports include a 'p' type byte followed by the total bytes processed so far
- Calls pq_flush_if_writable after progress reports to ensure timely delivery to clients

## Simplified Source

```c
static void
bbsink_copystream_archive_contents(bbsink *sink, size_t len)
{
    bbsink_copystream *mysink = (bbsink_copystream *) sink;
    bbsink_state *state = mysink->base.bbs_state;
    StringInfoData buf;
    uint64 targetbytes;

    // Send archive content to client if enabled
    if (mysink->send_to_client) {
        pq_putmessage('d', mysink->msgbuffer, len + 1);  // +1 for type byte
    }

    // Check if it's time for a progress report (after enough bytes processed)
    targetbytes = mysink->bytes_done_at_last_time_check + PROGRESS_REPORT_BYTE_INTERVAL;
    if (targetbytes <= state->bytes_done) {
        TimestampTz now = GetCurrentTimestamp();
        long ms;

        // Check time since last progress report
        mysink->bytes_done_at_last_time_check = state->bytes_done;
        ms = TimestampDifferenceMilliseconds(mysink->last_progress_report_time, now);

        // Send progress report if enough time elapsed or clock went backward
        if (ms >= PROGRESS_REPORT_MILLISECOND_THRESHOLD || now < mysink->last_progress_report_time) {
            mysink->last_progress_report_time = now;

            // Send progress message with current byte count
            pq_beginmessage(&buf, PqMsg_CopyData);
            pq_sendbyte(&buf, 'p');  // 'p' = Progress report
            pq_sendint64(&buf, state->bytes_done);
            pq_endmessage(&buf);
            pq_flush_if_writable();
        }
    }
}
```