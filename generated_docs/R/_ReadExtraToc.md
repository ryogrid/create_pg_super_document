# _ReadExtraToc

## Location
src/bin/pg_dump/pg_backup_custom.c: 238 - 264

## Overview
_ReadExtraToc is a callback function that reads format-specific TOC entry data from the archive, reconstructing data position offset information for the custom archive format during restoration.

## Definition


## Detailed Description
This function serves as the counterpart to _WriteExtraToc, reading back the format-specific data that was previously written to the archive. It's called by the PostgreSQL archiver during the archive loading process to restore the data position and state information for each Table of Contents (TOC) entry.

The function first checks if a local TOC entry context (lclTocEntry) already exists for the given TOC entry. If not, it allocates and initializes a new one. It then uses the archiver's ReadOffset routine to read the data position and state information from the archive file.

The function also handles backward compatibility with older archive versions. Prior to version 1.7 (PostgreSQL 7.3), archives included an additional data size field that is no longer used. For these older archives, the function reads and discards this obsolete integer value to maintain compatibility.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure containing archive context and I/O functions
- : Pointer to the TocEntry structure whose extra format data needs to be read

## Dependencies
- Functions called/Symbols referenced:
  - [ReadOffset](ReadOffset.md) (archiver utility function for reading offset data)
  - pg_malloc0 (memory allocation for new context)
  - [ReadInt](ReadInt.md) (archiver utility for reading integer values, used for compatibility)
  - lclTocEntry (local TOC entry structure type)
  - K_VERS_1_7 (version constant for compatibility checking)

- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (assigned as ReadExtraTocPtr function pointer)
  - Referenced by InitArchiveFmt_Directory (directory format also uses this pattern)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_backup_custom.c file
- The function is optional in the archive format interface but is essential for custom format restoration
- Must read data in the exact same order that _WriteExtraToc writes it to maintain file format integrity
- Handles backward compatibility with pre-1.7 archive versions by reading and discarding obsolete data size fields
- The restored offset information enables efficient seeking to data blocks during restoration
- If a formatData context doesn't exist, the function creates one, making it robust for various initialization scenarios
- Uses archiver-provided routines to ensure proper endianness handling and consistent file format interpretation