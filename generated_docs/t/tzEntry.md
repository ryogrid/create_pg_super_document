# tzEntry

## Location
src/include/utils/tzparser.h: 23 - 34

## Overview
The `tzEntry` struct represents a parsed timezone abbreviation entry from timezone configuration files, containing both timezone offset data and source location information for error reporting.

## Definition
```c
typedef struct tzEntry
{
    /* the actual data */
    char       *abbrev;         /* TZ abbreviation (downcased) */
    char       *zone;           /* zone name if dynamic abbrev, else NULL */
    /* for a dynamic abbreviation, offset/is_dst are not used */
    int         offset;         /* offset in seconds from UTC */
    bool        is_dst;         /* true if a DST abbreviation */
    /* source information (for error messages) */
    int         lineno;
    const char *filename;
} tzEntry;
```

## Detailed Description
The `tzEntry` structure is the fundamental data type used by PostgreSQL's timezone parser system to represent individual timezone abbreviation entries. It serves as the intermediate format between raw timezone configuration file data and the internal `TimeZoneAbbrevTable` structure used by the datetime system.

Each `tzEntry` represents either a static timezone abbreviation (with fixed offset and DST flag) or a dynamic abbreviation that references a timezone name. The structure includes both the parsed timezone data and metadata about where the entry was defined in the source file, enabling precise error reporting during configuration validation.

The parser maintains arrays of these structures sorted alphabetically by abbreviation name, which are later converted into the optimized `TimeZoneAbbrevTable` format for runtime timezone lookups.

## Parameters / Member Variables
- `abbrev`: The timezone abbreviation string (automatically converted to lowercase to match datetime.c conventions). Must not exceed TOKMAXLEN characters.
- `zone`: For dynamic abbreviations, contains the timezone name (e.g., "America/New_York"). NULL for static abbreviations.
- `offset`: UTC offset in seconds for static abbreviations. Valid range is ±14 hours (±50,400 seconds). Not used for dynamic abbreviations.
- `is_dst`: Boolean flag indicating whether this is a daylight saving time abbreviation for static entries. Not used for dynamic abbreviations.
- `lineno`: Line number in the source file where this entry was defined, used for error reporting.
- `filename`: Name of the source configuration file containing this entry, used for error reporting.

## Dependencies
- Functions called/Symbols referenced:
  - zone (member reference)
  
- Called from (representative examples):
  - `ConvertTimeZoneAbbrevs` (datetime.c:4873, 4886, 4912)
  - `validateTzEntry` (tzparser.c:52)
  - `splitTzLine` (tzparser.c:98)
  - `addToArray` (tzparser.c:188, 189, 191, 206, 252, 257, 259)
  - `ParseTzFile` (tzparser.c:277, 284)
  - `load_tzoffsets` (tzparser.c:452, 467)

## Notes and Other Information
- The structure is exported from tzparser.h because it is needed by datetime.c for timezone abbreviation processing
- Abbreviations are automatically converted to lowercase to ensure consistent matching with the datetime token system
- For dynamic abbreviations (where zone != NULL), the offset and is_dst fields are ignored as the actual values are determined by the referenced timezone
- The parser enforces validation rules including maximum abbreviation length and reasonable offset ranges (±14 hours)
- Arrays of tzEntry structs are maintained in sorted order by abbreviation name using strcmp() to match the sort order expected by datetime.c
- Source file information (filename, lineno) enables precise error reporting during GUC validation and helps administrators locate configuration issues
- The structure serves as input to `ConvertTimeZoneAbbrevs()` which transforms the parsed data into the runtime `TimeZoneAbbrevTable` format