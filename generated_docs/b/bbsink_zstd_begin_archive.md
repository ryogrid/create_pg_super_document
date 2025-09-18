# bbsink_zstd_begin_archive

## Location
src/backend/backup/basebackup_zstd.c: 158 - 192

## Overview
Prepares the zstd compression sink for compressing a new archive by resetting the compression context and setting up output buffers with an updated archive name.

## Definition


## Detailed Description
This function prepares the zstd compression sink to begin compressing a new archive file. It resets the zstd compression context using session-only reset to maintain compression parameters while starting fresh compression state. The function configures the output buffer to point to the next sink's buffer and appends the ".zst" extension to the archive name to indicate zstd compression before passing it to the next sink in the chain.

## Parameters / Member Variables
- : Pointer to the bbsink structure (cast to bbsink_zstd internally) that will perform zstd compression  
- : Name of the archive being compressed (without extension)

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_CCtx_reset (resets compression context for new archive)
  - psprintf (formats string with .zst extension)
  - bbsink_begin_archive (notifies next sink to begin archive)
  - pfree (frees formatted archive name)
  - Assert (validates next sink is not NULL)
- Called from (representative examples):
  - Through bbsink_zstd_ops function pointer table

## Notes and Other Information
- Uses ZSTD_reset_session_only to preserve compression parameters across archives
- Automatically appends ".zst" extension to archive names to indicate compression
- Sets up zstd output buffer to write directly to the next sink's buffer
- Resets buffer position to 0 for new compression operation
- Function is static and called through the bbsink operations table
- Compression parameters (level, workers, etc.) persist across archive boundaries due to session-only reset