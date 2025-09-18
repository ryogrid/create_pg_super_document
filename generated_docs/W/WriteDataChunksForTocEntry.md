# WriteDataChunksForTocEntry

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2556 - 2588

## Overview
Handles the data dumping process for a specific Table of Contents entry, coordinating the start/end callbacks and invoking the appropriate data dumper routine.

## Definition
```c
void WriteDataChunksForTocEntry(ArchiveHandle *AH, TocEntry *te)
```

## Detailed Description
This function orchestrates the data dumping process for a single database object represented by a TocEntry. It sets up the archive handle's current TOC reference, determines the appropriate start and end pointer functions based on whether the entry is for BLOBs or regular data, and coordinates the execution of pre-dump, dump, and post-dump operations. The function acts as a wrapper that provides a consistent interface for data dumping regardless of the specific type of database object being processed.

## Parameters / Member Variables
- `AH`: Archive handle containing the dump state, configuration, and function pointers for the dumping process
- `te`: Table of Contents entry representing the database object to be dumped, containing metadata and the data dumper function

## Dependencies
- Functions called/Symbols referenced:
  - TocEntry (struct type)
  - StartDataPtrType and EndDataPtrType function pointers
  - dataDumper callback function
- Called from (representative examples):
  - WriteDataChunks
  - _WorkerJobDumpDirectory

## Notes and Other Information
- The function distinguishes between BLOB data and regular table data, using different start/end function pointers accordingly
- It temporarily sets AH->currToc to the current entry being processed and clears it when done
- The actual data dumping is delegated to the te->dataDumper function, which is expected to call AH->WriteData
- This function provides a standardized framework for data dumping that can be used in both sequential and parallel dump operations
- The start and end pointer functions are optional (can be NULL) and are used for format-specific initialization and cleanup