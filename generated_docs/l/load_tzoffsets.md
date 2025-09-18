# load_tzoffsets

## Location
src/backend/utils/misc/tzparser.c: 447 - 485

## Overview
Main entry point for loading and parsing timezone offset files, returning a complete TimeZoneAbbrevTable structure.

## Definition
TimeZoneAbbrevTable *load_tzoffsets(const char *filename)

## Detailed Description
This function serves as the high-level interface for loading PostgreSQL timezone abbreviation data from files. It creates a temporary memory context for parsing operations, initializes the timezone entry array, calls ParseTzFile to process the file, and then converts the parsed data into the final TimeZoneAbbrevTable format used by PostgreSQL's datetime system. The function handles memory management carefully, cleaning up temporary allocations regardless of success or failure.

## Parameters / Member Variables
- : Name of the timezone abbreviation file to load

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - CurrentMemoryContext
  - ALLOCSET_SMALL_SIZES
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - [ParseTzFile](../P/ParseTzFile.md)
  - [ConvertTimeZoneAbbrevs](../C/ConvertTimeZoneAbbrevs.md)
  - GUC_check_errmsg
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [check_timezone_abbreviations](../c/check_timezone_abbreviations.md)

## Notes and Other Information
The function returns a complete TimeZoneAbbrevTable on success, or NULL on failure with appropriate error messages set via GUC_check_errmsg. The returned table must be allocated with guc_malloc (not palloc) for proper memory management in PostgreSQL's GUC system. The temporary memory context ensures clean cleanup of all intermediate allocations.