# hash_xlog_split_page

## Location
src/backend/access/hash/hash_xlog.c: 428 - 441

## Overview
Replays a hash index split operation during WAL (Write-Ahead Log) recovery, restoring the split page from a full-page image.

## Definition
static void hash_xlog_split_page(XLogReaderState *record)

## Detailed Description
This function handles the replay of a hash index split operation during PostgreSQL's crash recovery process. When a hash index bucket needs to be split due to growth, the operation is logged to WAL. During recovery, this function is called to restore the split page state. The function expects the WAL record to contain a full-page image of the split page, which is then restored directly without any additional processing.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record data for the split operation

## Dependencies
- Functions called/Symbols referenced:
  - XLogReadBufferForRedo
  - BLK_RESTORED
  - elog
  - UnlockReleaseBuffer
- Called from (representative examples):
  - hash_redo

## Notes and Other Information
- This is a static function used only within the hash WAL recovery module
- The function assumes the WAL record contains a full-page image and will throw an ERROR if this expectation is not met
- Part of PostgreSQL's hash index WAL recovery infrastructure
- The simplicity of this function reflects that split operations are logged as complete page images rather than incremental changes