# bbsink_copystream_end_backup

## Location
src/backend/backup/basebackup_copy.c: 297 - 307

## Overview
Sends end-of-backup wire protocol messages to complete a copystream-based backup operation.

## Definition
static void bbsink_copystream_end_backup(bbsink *sink, XLogRecPtr endptr, TimeLineID endtli)

## Detailed Description
This function finalizes the backup process by sending the necessary wire protocol messages to indicate backup completion. It calls SendCopyDone() to send a CopyDone message indicating the end of the COPY operation, followed by SendXlogRecPtrResult() to transmit the final WAL position and timeline information back to the client. These messages are essential for proper backup protocol completion and allow the client to know the exact point where the backup ended.

## Parameters / Member Variables
- `sink`: Pointer to the base bbsink structure representing the copystream backup sink (unused in implementation)
- `endptr`: XLogRecPtr indicating the final WAL position where the backup ended
- `endtli`: TimeLineID specifying the timeline at backup completion

## Dependencies
- Functions called/Symbols referenced:
  - [SendCopyDone](../S/SendCopyDone.md)
  - [SendXlogRecPtrResult](../S/SendXlogRecPtrResult.md)
- Called from (representative examples):
  - Used as callback function during backup finalization in bbsink copystream operations

## Notes and Other Information
- This is a static function internal to the basebackup_copy.c module
- Part of the bbsink copystream implementation for PostgreSQL base backups
- Essential for proper backup protocol completion
- Sends both the end-of-copy signal and the final WAL position information
- The endptr and endtli parameters provide critical information for backup consistency
- Located in src/backend/backup/basebackup_copy.c:297-307