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
  - [pglz_decompress](../p/pglz_decompress.md) (PGLZ decompression)
  - LZ4_decompress_safe (LZ4 decompression, if built with LZ4)
  - ZSTD_decompress (ZSTD decompression, if built with ZSTD)
  - MemSet (memory zeroing utility)
- Called from (representative examples):
  - [verifyBackupPageConsistency](../v/verifyBackupPageConsistency.md)
  - [XLogReadBufferForRedoExtended](../X/XLogReadBufferForRedoExtended.md)
  - [XLogRecordSaveFPWs](../X/XLogRecordSaveFPWs.md)
  - XLogRecHasBlockData

## Notes and Other Information
- Returns true on successful restoration, false on failure with error details reported via report_invalid_record
- Supports three compression methods: PGLZ (always available), LZ4 (optional), and ZSTD (optional)
- Handles "holes" in backup images - regions that can be zero-filled to reduce storage requirements
- The output page buffer must be exactly BLCKSZ bytes in size
- Essential for crash recovery and point-in-time recovery operations
- Used extensively during WAL replay to restore full pages when incremental changes are insufficient

## Simplified Source
```c
bool RestoreBlockImage(XLogReaderState *record, uint8 block_id, char *page)
{
    DecodedBkpBlock *bkpb;
    char *ptr;
    PGAlignedBlock tmp;

    // Validate block ID and check for image
    if (block_id > record->record->max_block_id ||
        !record->record->blocks[block_id].in_use) {
        report_invalid_record(record, "invalid block %d specified", block_id);
        return false;
    }

    if (!record->record->blocks[block_id].has_image) {
        report_invalid_record(record, "block %d has no image", block_id);
        return false;
    }

    bkpb = &record->record->blocks[block_id];
    ptr = bkpb->bkp_image;

    // Handle compressed images
    if (BKPIMAGE_COMPRESSED(bkpb->bimg_info)) {
        bool decomp_success = true;

        if (bkpb->bimg_info & BKPIMAGE_COMPRESS_PGLZ) {
            // Decompress using PGLZ
            if (pglz_decompress(ptr, bkpb->bimg_len, tmp.data,
                               BLCKSZ - bkpb->hole_length, true) < 0) {
                decomp_success = false;
            }
        } else if (bkpb->bimg_info & BKPIMAGE_COMPRESS_LZ4) {
            // Decompress using LZ4 (if available)
#ifdef USE_LZ4
            if (LZ4_decompress_safe(ptr, tmp.data, bkpb->bimg_len,
                                   BLCKSZ - bkpb->hole_length) <= 0) {
                decomp_success = false;
            }
#else
            report_invalid_record(record, "LZ4 compression not supported");
            return false;
#endif
        } else if (bkpb->bimg_info & BKPIMAGE_COMPRESS_ZSTD) {
            // Decompress using ZSTD (if available)
#ifdef USE_ZSTD
            size_t result = ZSTD_decompress(tmp.data, BLCKSZ - bkpb->hole_length,
                                           ptr, bkpb->bimg_len);
            if (ZSTD_isError(result)) {
                decomp_success = false;
            }
#else
            report_invalid_record(record, "ZSTD compression not supported");
            return false;
#endif
        } else {
            report_invalid_record(record, "unknown compression method");
            return false;
        }

        if (!decomp_success) {
            report_invalid_record(record, "decompression failed for block %d", block_id);
            return false;
        }

        ptr = tmp.data;
    }

    // Restore page, handling holes if present
    if (bkpb->hole_length == 0) {
        // No hole, simple copy
        memcpy(page, ptr, BLCKSZ);
    } else {
        // Copy around hole and zero-fill hole region
        memcpy(page, ptr, bkpb->hole_offset);
        MemSet(page + bkpb->hole_offset, 0, bkpb->hole_length);
        memcpy(page + (bkpb->hole_offset + bkpb->hole_length),
               ptr + bkpb->hole_offset,
               BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
    }

    return true;
}
```