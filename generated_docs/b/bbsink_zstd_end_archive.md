# bbsink_zstd_end_archive

## Location
src/backend/backup/basebackup_zstd.c: 235 - 281

## Overview
Finalizes zstd compression by flushing internal buffers, ending the compression frame, and forwarding any remaining compressed data to the next sink in the chain.

## Definition


## Detailed Description
This function completes the zstd compression for an archive by flushing any remaining data from zstd's internal buffers and properly ending the compression frame. It uses ZSTD_e_end mode to signal the end of compression, which causes zstd to flush all buffered data and write frame termination markers. The function continues compressing until no more data needs to be flushed, handles output buffer management by sending data to the next sink when space is needed, and ensures any final compressed bytes are forwarded before notifying the next sink that the archive has ended.

## Parameters / Member Variables
- : Pointer to the bbsink structure (cast to bbsink_zstd internally) that contains compression context and buffers

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_compressBound (calculates space needed for final compression)
  - ZSTD_compressStream2 (performs final compression with ZSTD_e_end mode)
  - ZSTD_isError (checks for compression errors)
  - ZSTD_getErrorName (gets error description)  
  - bbsink_archive_contents (sends compressed data to next sink)
  - bbsink_forward_end_archive (notifies next sink that archive ended)
  - elog (error logging)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Uses ZSTD_e_end mode to signal compression completion and flush internal buffers
- Loops until yet_to_flush returns 0, indicating all buffered data has been output
- Uses empty input buffer (NULL, 0, 0) since no new data is being compressed
- Manages output buffer space by flushing to next sink when needed
- Ensures any remaining bytes in output buffer are sent to next sink before ending
- Calls bbsink_forward_end_archive to properly terminate the archive in the sink chain
- Function is static and called through the bbsink operations table
- Critical for proper zstd frame termination and ensuring no compressed data is lost