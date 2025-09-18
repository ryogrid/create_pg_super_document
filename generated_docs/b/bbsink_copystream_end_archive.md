# bbsink_copystream_end_archive

## Location
src/backend/backup/basebackup_copy.c: 241 - 259

## Overview
Forces a final progress report to the client at the end of an archive transmission, ensuring accurate progress reporting.

## Definition
static void bbsink_copystream_end_archive(bbsink *sink)

## Detailed Description
This function is called when an archive transmission is complete, but does not explicitly signal the end of the archive in the protocol stream since the client can infer the end when the next archive begins, the manifest starts, or the COPY stream ends. Instead, it focuses on providing accurate progress reporting by forcing a progress report that reflects the total bytes processed so far. This ensures that clients receive complete progress information, particularly important for the last archive where no subsequent archive would trigger a final progress update. The function updates the progress tracking timestamps and sends a CopyData message with current progress.

## Parameters / Member Variables
- : Pointer to the base bbsink structure (cast to bbsink_copystream internally)

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTimestamp
  - pq_beginmessage
  - pq_sendbyte
  - pq_sendint64
  - pq_endmessage
  - pq_flush_if_writable
  - PqMsg_CopyData
  - bbsink_copystream
  - bbsink_state
- Called from (representative examples):
  - Referenced by bbsink_copystream_ops structure as the end_archive handler

## Notes and Other Information
- Does not send an explicit archive termination message since the protocol relies on implicit boundaries
- Always forces a progress report regardless of time or byte thresholds, ensuring complete progress tracking
- Updates both bytes_done_at_last_time_check and last_progress_report_time to current values
- Sends a 'p' type byte followed by the total bytes processed (state->bytes_done)
- Critical for final progress reporting when this is the last archive in the backup
- Uses pq_flush_if_writable to ensure the progress message reaches the client promptly