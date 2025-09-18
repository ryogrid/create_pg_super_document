# EndCompressor

## Location
src/bin/pg_dump/compress_io.c: 149 - 163

## Overview
This function terminates the compression library context, flushes any remaining buffers, and frees the CompressorState structure.

## Definition


## Detailed Description
The `EndCompressor` function provides a clean shutdown mechanism for compression operations. It calls the algorithm-specific end function through the CompressorState's function pointer to properly terminate the compression context, flush any remaining data in internal buffers, and perform necessary cleanup. After the algorithm-specific cleanup is complete, it frees the memory allocated for the CompressorState structure itself.

This function serves as the counterpart to `AllocateCompressor` and should be called when compression operations are complete to ensure proper resource cleanup and prevent memory leaks.

## Parameters / Member Variables
- `AH`: Pointer to ArchiveHandle structure containing archive context information
- `cs`: Pointer to CompressorState structure to be terminated and freed

## Dependencies
- Functions called/Symbols referenced:
  - cs->end (function pointer call to algorithm-specific end function)
  - pg_free
  - CompressorState
- Called from (representative examples):
  - _EndData (src/bin/pg_dump/pg_backup_custom.c:333)
  - _EndLO (src/bin/pg_dump/pg_backup_custom.c:395)
  - _PrintData (src/bin/pg_dump/pg_backup_custom.c:576)

## Notes and Other Information
- This function must be called to properly clean up resources allocated by AllocateCompressor
- The cs->end function pointer is set during initialization by the algorithm-specific Init functions
- Failure to call this function will result in memory leaks and potentially incomplete compression output
- The function handles the final steps of the compression process including flushing any remaining buffered data
- Located in src/bin/pg_dump/compress_io.c at lines 149-163