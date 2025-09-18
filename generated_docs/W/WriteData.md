# WriteData

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1201 - 1221

## Overview
WriteData is a public interface function that provides a safe way to write binary data during PostgreSQL database dumping operations, ensuring the data is written within the proper context of a DataDumper routine.

## Definition


## Detailed Description
WriteData serves as a wrapper function in the pg_dump archiver interface that validates the current dumping context and delegates the actual data writing to the appropriate backend-specific write function. The function ensures that data can only be written when there is an active table of contents (TOC) entry being processed, preventing invalid write operations outside of proper dumping contexts.

The function performs a critical safety check by verifying that  is not NULL before proceeding with the write operation. If called outside the context of a DataDumper routine, it terminates the program with a fatal error. Upon successful validation, it delegates the actual writing to the function pointer , which points to the format-specific implementation (e.g., for custom, tar, or directory formats).

## Parameters / Member Variables
- : Archive pointer (cast from ArchiveHandle) representing the current dump session
- : Pointer to the binary data to be written to the archive
- : Size in bytes of the data to be written

## Dependencies
- Functions called/Symbols referenced:
  - pg_fatal (for error handling)
  - AH->WriteDataPtr (function pointer for format-specific writing)
- Called from (representative examples):
  - archputs
  - archprintf  
  - dumpTableData_copy
  - dumpLOs

## Notes and Other Information
- This function is part of the public dumper interface and must only be called within DataDumper routines
- The function provides a layer of safety by validating the dumping context before delegating to format-specific implementations
- Error handling is strict - any attempt to call this function outside proper context results in program termination
- The actual writing mechanism varies depending on the archive format (custom, tar, directory, etc.) through the WriteDataPtr function pointer