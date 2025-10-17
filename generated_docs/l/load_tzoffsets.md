# load_tzoffsets

## Location
[src/backend/utils/misc/tzparser.c:447-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L447-L485)

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

## Simplified Source

```c
TimeZoneAbbrevTable *load_tzoffsets(const char *filename) {
    TimeZoneAbbrevTable *result = NULL;
    MemoryContext tmpContext;
    MemoryContext oldContext;
    tzEntry *array;
    int arraysize;
    int n;

    // Create temporary memory context for parsing
    tmpContext = AllocSetContextCreate(CurrentMemoryContext,
                                      "TZParserMemory",
                                      ALLOCSET_SMALL_SIZES);
    oldContext = MemoryContextSwitchTo(tmpContext);

    // Initialize timezone entry array
    arraysize = 128;
    array = (tzEntry *) palloc(arraysize * sizeof(tzEntry));

    // Parse the timezone file(s)
    n = ParseTzFile(filename, 0, &array, &arraysize, 0);

    // Convert parsed data to final format
    if (n >= 0) {
        result = ConvertTimeZoneAbbrevs(array, n);
        if (!result)
            GUC_check_errmsg("out of memory");
    }

    // Clean up temporary context
    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(tmpContext);

    return result;
}
```