# TimeZoneAbbrevTable

## Location
[src/include/utils/datetime.h:215-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/datetime.h#L215-L221)

## Overview
TimeZoneAbbrevTable is a data structure that stores a table of time zone abbreviations, providing efficient lookup and storage of timezone abbreviation information in PostgreSQL.

## Definition

Source: src/include/utils/datetime.h:214-221

## Detailed Description
TimeZoneAbbrevTable serves as a container for time zone abbreviation data in PostgreSQL's datetime processing system. The structure uses a flexible array member design to efficiently store variable numbers of timezone abbreviations in a single contiguous memory block. The table can optionally include dynamic zone abbreviations that follow the main abbreviations array. This design allows for efficient memory usage and fast lookup operations when processing datetime values with timezone abbreviations.

## Parameters / Member Variables
- : Total size in bytes of the entire TimeZoneAbbrevTable structure, including the flexible array
- : The number of timezone abbreviation entries stored in the abbrevs[] array
- : Flexible array member containing datetkn structures, each representing a timezone abbreviation with its token, type, and value information

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array implementation)
  - datetkn (structure type for storing individual timezone abbreviation data)

- Called from (representative examples):
  - [assign_timezone_abbreviations](../a/assign_timezone_abbreviations.md) (src/backend/commands/variable.c:523)
  - [ConvertTimeZoneAbbrevs](../C/ConvertTimeZoneAbbrevs.md) (src/backend/utils/adt/datetime.c:4875,4880,4907)
  - [InstallTimeZoneAbbrevs](../I/InstallTimeZoneAbbrevs.md) (src/backend/utils/adt/datetime.c:4957)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (src/backend/utils/adt/datetime.c:4970)
  - [ParseTzFile](../P/ParseTzFile.md) (src/backend/utils/misc/tzparser.c:446)
  - [load_tzoffsets](../l/load_tzoffsets.md) (src/backend/utils/misc/tzparser.c:449)

## Notes and Other Information
- The structure uses a flexible array member pattern, allowing the actual size to vary based on the number of abbreviations stored
- [DynamicZoneAbbrev](../D/DynamicZoneAbbrev.md) structures may follow the main abbrevs[] array in memory, providing extensibility for dynamic timezone data
- This is a core component of PostgreSQL's timezone abbreviation handling system, used throughout the datetime processing subsystem
- The design prioritizes memory efficiency and lookup performance for timezone abbreviation resolution