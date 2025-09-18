# InitArchiveFmt_Null

## Location
src/bin/pg_dump/pg_backup_null.c: 48 - 80

## Overview
Initializes the "null" archive format handler in pg_dump, which provides a write-only format that discards all data (used for testing and benchmarking purposes).

## Definition
```c
void InitArchiveFmt_Null(ArchiveHandle *AH)
```

## Detailed Description
InitArchiveFmt_Null sets up the function pointers in an ArchiveHandle structure to implement the "null" archive format. This format is designed as a write-only sink that discards all data, making it useful for performance testing and benchmarking pg_dump operations without the overhead of actual I/O operations. The function assigns specific null format implementations to all the required archive operations and explicitly prevents read operations by checking the archive mode and throwing a fatal error if reading is attempted.

## Parameters / Member Variables
- `AH`: Pointer to the ArchiveHandle structure that will be configured for the null format operations

## Dependencies
- Functions called/Symbols referenced:
  - [_WriteData](../W/_WriteData.md)
  - [_EndData](../E/_EndData.md)
  - [_WriteByte](../W/_WriteByte.md)
  - [_WriteBuf](../W/_WriteBuf.md)
  - [_CloseArchive](../C/_CloseArchive.md)
  - [_PrintTocData](../P/_PrintTocData.md)
  - [_StartLOs](../S/_StartLOs.md)
  - [_StartLO](../S/_StartLO.md)
  - [_EndLO](../E/_EndLO.md)
  - [_EndLOs](../E/_EndLOs.md)
  - archModeRead (constant)
- Called from (representative examples):
  - [_allocAH](../a/_allocAH.md)
  - appendByteaLiteralAHX

## Notes and Other Information
- This format explicitly cannot be read - attempting to open a null format archive in read mode will result in a fatal error
- The null format sets ClonePtr and DeClonePtr to NULL, indicating no support for parallel dump operations
- ReopenPtr is also set to NULL, meaning the format doesn't support reopening closed archives
- This format is primarily used for testing pg_dump performance without I/O bottlenecks