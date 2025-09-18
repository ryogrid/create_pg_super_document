# _PrintExtraToc

## Location
src/bin/pg_dump/pg_backup_custom.c: 265 - 284

## Overview
_PrintExtraToc is a callback function that outputs diagnostic information about TOC entries during archive restoration, specifically printing data position information when verbose mode is enabled.

## Definition


## Detailed Description
This function serves as an optional callback invoked by the PostgreSQL archiver during restoration operations to provide additional diagnostic information about Table of Contents (TOC) entries. When the verbose flag is enabled in the archive handle, this function outputs the data position offset for each TOC entry.

The function retrieves the local TOC entry context (lclTocEntry) that contains the format-specific data, including the data position (dataPos) that was previously read by _ReadExtraToc. It then uses the archiver's ahprintf routine to output a comment line showing the data position in the archive file.

This diagnostic information is valuable for debugging archive issues, understanding archive structure, and verifying that data positions are correctly maintained throughout the archive creation and restoration process.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure containing archive context and output functions
- : Pointer to the TocEntry structure whose extra format information should be printed

## Dependencies
- Functions called/Symbols referenced:
  - [ahprintf](../a/ahprintf.md) (archiver utility function for formatted output)
  - lclTocEntry (local TOC entry structure type)
  - INT64_FORMAT (format specifier for 64-bit integers)

- Called from (representative examples):
  - [InitArchiveFmt_Custom](../I/InitArchiveFmt_Custom.md) (assigned as PrintExtraTocPtr function pointer)
  - Referenced by InitArchiveFmt_Directory (directory format also uses this pattern)

## Notes and Other Information
- This function is declared as static, limiting its scope to the pg_backup_custom.c file
- The function is optional in the archive format interface and provides diagnostic value rather than functional necessity
- Output is conditional on the verbose flag (AH->public.verbose), making it suitable for debugging without cluttering normal output
- The data position is printed as a 64-bit integer using INT64_FORMAT for cross-platform compatibility
- Uses ahprintf to ensure output is properly formatted and directed to the appropriate output stream
- The comment format ("-- Data Pos: ...") follows SQL comment conventions, making it safe to include in SQL output
- This function complements the other Extra TOC functions (_WriteExtraToc and _ReadExtraToc) by providing visibility into their operation