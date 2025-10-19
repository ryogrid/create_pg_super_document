# rdatestr

## Location
[src/interfaces/ecpg/compatlib/informix.c:508-528](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L508-L528)

## Overview
Converts a date value to its string representation, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition
```c
int rdatestr(date d, char *str)
```

## Detailed Description
The `rdatestr` function is part of PostgreSQL's ECPG date handling compatibility library for Informix. It converts a date value to its ASCII string representation using PostgreSQL's internal date conversion routines. The function allocates temporary memory for the conversion, copies the result to the user-provided buffer, and properly cleans up the temporary allocation. This provides a simple interface for converting PostgreSQL date types to string format in a way that's compatible with Informix applications.

## Parameters / Member Variables
- `d`: The input date value to be converted to string format
- `str`: Pointer to a user-allocated character buffer where the date string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESdate_to_asc](../P/PGTYPESdate_to_asc.md)
  - strcpy
  - free
  - ECPG_INFORMIX_DATE_CONVERT (error constant)
- Called from (representative examples):
  - [date_test_strdate](../d/date_test_strdate.md) (in test programs)
  - [date_test_defmt](../d/date_test_defmt.md) (in test programs)
  - [main](../m/main.md) (in test programs)
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Returns 0 on success, ECPG_INFORMIX_DATE_CONVERT on failure
- User must provide a sufficiently large buffer for the date string
- Handles memory management internally by freeing the temporary string
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:508-528
- Simple wrapper around PostgreSQL's native date-to-ASCII conversion
- Assumes the user-provided buffer has adequate space for the date string

## Simplified Source

```c
int rdatestr(date d, char *str) {
    // Convert date to ASCII string using PostgreSQL's built-in function
    char *tmp = PGTYPESdate_to_asc(d);

    // Check if conversion succeeded
    if (!tmp)
        return ECPG_INFORMIX_DATE_CONVERT;

    // Copy result to user buffer and cleanup
    strcpy(str, tmp);
    free(tmp);

    return 0;  // Success
}
```