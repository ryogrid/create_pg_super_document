# ConvertTimeZoneAbbrevs

## Location
[src/backend/utils/adt/datetime.c:4873-4956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/datetime.c#L4873-L4956)

## Overview
ConvertTimeZoneAbbrevs converts a sorted array of timezone abbreviation entries into a finalized TimeZoneAbbrevTable structure used for timezone abbreviation lookup during timestamp parsing.

## Definition


## Detailed Description
This function is called during timezone configuration file loading or reloading to create the runtime timezone abbreviation table. It processes a pre-sorted array of tzEntry structures and builds a single, contiguously allocated TimeZoneAbbrevTable.

The function performs two main tasks:
1. **Space calculation**: First pass calculates the total memory needed for the table including both static datetkn entries and variable-length dynamic zone abbreviations
2. **Table construction**: Second pass populates the allocated memory with datetkn entries and DynamicZoneAbbrev structures

The function handles two types of timezone abbreviations:
- **Static abbreviations**: Fixed-offset timezones (like EST, PST) stored directly in datetkn entries
- **Dynamic abbreviations**: Zone-based abbreviations (like America/New_York) that require runtime timezone lookup via DynamicZoneAbbrev structures

The result is a single guc_malloc'd memory chunk containing the complete timezone abbreviation lookup table.

## Parameters / Member Variables
- : Array of tzEntry structures containing timezone abbreviation data, pre-sorted by name
- : Number of entries in the abbrevs array

## Dependencies
- Functions called/Symbols referenced:
  - [guc_malloc](../g/guc_malloc.md) (GUC memory allocation)
  - strlcpy (safe string copy with truncation)
  - strcpy (string copy)
  - strlen (string length)
  - MAXALIGN (memory alignment macro)
  - [CheckDateTokenTable](CheckDateTokenTable.md) (debug validation function)
- Data structures referenced:
  - tzEntry (input timezone entry structure)
  - TimeZoneAbbrevTable (output table structure)
  - datetkn (timezone token structure)
  - DynamicZoneAbbrev (dynamic timezone abbreviation structure)
- Called from (representative examples):
  - [load_tzoffsets](../l/load_tzoffsets.md) (src/backend/utils/misc/tzparser.c:475)

## Notes and Other Information
- Returns NULL only on memory allocation failure; no other error conditions are defined
- The function assumes the input array is already sorted by abbreviation name for efficient binary search lookup
- Dynamic zone abbreviations are stored as offsets from the table start to enable the entire table to be relocated as a single memory chunk
- TOKMAXLEN limits abbreviation names to prevent buffer overflows
- Uses two-pass allocation strategy to ensure exact memory usage calculation
- All dynamic abbreviations initially have NULL tz pointers - these are resolved lazily during first use