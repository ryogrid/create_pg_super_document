# db_encoding_convert

## Location
[src/backend/utils/adt/pg_locale.c:517-546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L517-L546)

## Overview
This static function converts a malloc'd string from a specified source encoding to the PostgreSQL database encoding, handling memory management and encoding conversion for locale-related strings.

## Definition

```c
static void
db_encoding_convert(int encoding, char **str)
```
## Detailed Description
The  function is a utility that ensures locale-related strings are properly converted to the database's character encoding. This is crucial for locale formatting information that may originate from the system locale, which could be in a different encoding than PostgreSQL's database encoding.

The function performs several key operations:
1. **Encoding conversion**: Uses  to convert from the source encoding to the database encoding
2. **Memory management**: Handles the transition from palloc'd memory (from conversion) to malloc'd memory (required for locale strings)
3. **Optimization**: Skips conversion if no encoding change is needed
4. **Error handling**: Reports out-of-memory errors if malloc fails
5. **Cleanup**: Properly frees both the original string and temporary conversion results

The function modifies the string in-place through the double pointer parameter, replacing the original string with the converted version when necessary.

## Parameters / Member Variables
- `encoding`: The source encoding ID of the input string
- `**str`: Pointer to a pointer to the string to be converted (modified in-place)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_any_to_server](../p/pg_any_to_server.md) (PostgreSQL's encoding conversion function)
  - strdup (standard C library function for string duplication)
  - free (standard C library function for memory deallocation)
  - [pfree](../p/pfree.md) (PostgreSQL's palloc memory deallocator)
  - ereport/errcode/errmsg (PostgreSQL's error reporting system)
- Called from (representative examples):
  - [PGLC_localeconv](../P/PGLC_localeconv.md) (multiple calls at lines 709, 710, 717, 718, 719, 720, 722, 723)

## Notes and Other Information
- The function is declared as , making it internal to pg_locale.c
- Requires malloc'd strings as input and maintains malloc'd output for consistency with locale handling
- Handles the palloc/malloc memory boundary carefully to avoid memory leaks
- Essential for ensuring locale formatting strings work correctly with database encoding
- Used extensively in PGLC_localeconv for converting locale-specific formatting strings
- Part of PostgreSQL's encoding infrastructure for internationalization support

## Simplified Source

```c
static void db_encoding_convert(int encoding, char **str) {
    // Convert string from source encoding to database encoding
    char *converted_str = pg_any_to_server(*str, strlen(*str), encoding);

    // Skip if no conversion needed
    if (converted_str == *str) {
        return;
    }

    // Duplicate to malloc'd memory (required for locale strings)
    char *malloc_str = strdup(converted_str);
    if (malloc_str == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY),
                       errmsg("out of memory")));
    }

    // Replace original string with converted version
    free(*str);
    *str = malloc_str;
    pfree(converted_str);
}
```