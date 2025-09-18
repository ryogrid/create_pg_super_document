# EndCompressFileHandle

## Location
src/bin/pg_dump/compress_io.c: 290 - 301

## Overview
Closes an open compression file handle and releases all associated memory, providing proper cleanup for compression resources across all supported formats.

## Definition
bool EndCompressFileHandle(CompressFileHandle *CFH)

## Detailed Description
This function provides a unified interface for closing compression file handles and cleaning up resources. It checks if the file handle has private data (indicating an active file), and if so, calls the compression-specific close function through the function pointer stored in the handle. After attempting to close the file, it frees the memory allocated for the CompressFileHandle structure itself. The function is designed to work with all supported compression formats by using the polymorphic close_func function pointer.

## Parameters / Member Variables
- CFH: The CompressFileHandle structure to close and deallocate

## Dependencies
- Functions called/Symbols referenced:
  - [free_keep_errno](../f/free_keep_errno.md)
  - CFH->close_func (function pointer call)
- Called from (representative examples):
  - [CloseArchive](../C/CloseArchive.md)
  - [RestoreOutput](../R/RestoreOutput.md)
  - [InitArchiveFmt_Directory](../I/InitArchiveFmt_Directory.md)
  - [_EndData](_EndData.md)
  - [_PrintFileData](../P/_PrintFileData.md)
  - [_LoadLOs](../L/_LoadLOs.md)
  - [_CloseArchive](../C/_CloseArchive.md)
  - [_EndLO](_EndLO.md)
  - [_EndLOs](_EndLOs.md)

## Notes and Other Information
The function returns a boolean indicating success or failure of the close operation, though the memory cleanup always occurs regardless of the close result. It uses free_keep_errno to preserve error codes that might be set by the compression-specific close function. The function safely handles cases where private_data is NULL, indicating no active file to close. This function serves as the complementary cleanup operation to InitCompressFileHandle and InitDiscoverCompressFileHandle.