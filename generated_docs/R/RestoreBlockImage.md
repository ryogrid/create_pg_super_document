# RestoreBlockImage

## Location
[src/backend/access/transam/xlogreader.c:2066-2176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L2066-L2176)

## Overview
Restores a full-page image from a backup block attached to an XLog record, handling compressed images and blocks with holes.

## Definition
```c
bool RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
```

## Detailed Description
RestoreBlockImage is a critical function in PostgreSQL WAL recovery that reconstructs complete database pages from full-page images (FPI) stored in WAL records. The function handles various compression methods (PGLZ, LZ4, ZSTD) and supports blocks with holes - regions that contain no meaningful data and can be zero-filled to save space.

The function performs extensive validation to ensure the block ID is valid and that the block actually contains a full-page image. It then processes the image data, decompressing it if necessary using the appropriate algorithm based on the compression flags. For blocks with holes, it carefully reconstructs the page by copying data around the hole region and zero-filling the hole itself.

The function supports multiple compression algorithms conditionally based on build configuration, providing appropriate error messages when unsupported compression methods are encountered.

## Parameters / Member Variables
- `record`: Pointer to XLogReaderState containing the decoded WAL record with the backup block
- `block_id`: Identifier of the block whose full-page image should be restored (0-based index)  
- `page`: Output buffer where the restored page will be written (must be BLCKSZ bytes)

## Dependencies
- Functions called/Symbols referenced:
  - DecodedBkpBlock (struct for accessing block backup information)
  - PGAlignedBlock (aligned buffer for decompression)
  - [report_invalid_record](../r/report_invalid_record.md) (error reporting)
  - BKPIMAGE_COMPRESSED and compression flag macros
  - pglz_decompress (PGLZ decompression)
  - LZ4_decompress_safe (LZ4 decompression, if built with LZ4)
  - ZSTD_decompress (ZSTD decompression, if built with ZSTD)
  - MemSet (memory zeroing utility)
- Called from (representative examples):
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - XLogReadBufferForRedoExtended
  - [XLogRecordSaveFPWs](../X/XLogRecordSaveFPWs.md)
  - XLogRecHasBlockData

## Notes and Other Information
- Returns true on successful restoration, false on failure with error details reported via report_invalid_record
- Supports three compression methods: PGLZ (always available), LZ4 (optional), and ZSTD (optional)
- Handles "holes" in backup images - regions that can be zero-filled to reduce storage requirements
- The output page buffer must be exactly BLCKSZ bytes in size
- Essential for crash recovery and point-in-time recovery operations
- Used extensively during WAL replay to restore full pages when incremental changes are insufficient