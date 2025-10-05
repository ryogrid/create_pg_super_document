# PGTYPESnumeric_from_double

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1411-1431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1411-L1431)

## Overview
Converts a double-precision floating-point value into PostgreSQL's numeric type representation by formatting to string and parsing back to numeric.

## Definition
```c
int PGTYPESnumeric_from_double(double d, numeric *dst)
```

## Detailed Description
This function converts a double-precision floating-point number to PostgreSQL's numeric format using a two-step process. First, it formats the double value into a string representation using sprintf with DBL_DIG precision to ensure all significant digits are captured. Then, it uses the string-to-numeric conversion function to parse the formatted string into the numeric representation. Finally, it copies the result to the destination and cleans up temporary resources.

The conversion process involves:
1. Formatting the double to a string with appropriate precision (DBL_DIG digits)
2. Converting the string representation to a temporary numeric value
3. Copying the temporary numeric to the destination
4. Freeing the temporary numeric and handling error conditions
5. Clearing errno to indicate successful completion

## Parameters / Member Variables
- `d`: The double-precision floating-point value to convert
- `dst`: Pointer to the numeric structure that will store the converted value

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_from_asc](PGTYPESnumeric_from_asc.md) (converts ASCII string to numeric)
  - [PGTYPESnumeric_copy](PGTYPESnumeric_copy.md) (copies numeric values)
  - [PGTYPESnumeric_free](PGTYPESnumeric_free.md) (frees numeric memory)
  - [numeric](../n/numeric.md) (type definition)
  - DBL_DIG (constant for double precision digits)
- Called from (representative examples):
  - [deccvdbl](../d/deccvdbl.md) (in informix compatibility library)
  - decimal (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (sprintf failure, conversion failure, or copy failure)
- Uses DBL_DIG + 100 character buffer to accommodate the formatted double value
- Leverages existing string-to-numeric conversion infrastructure for consistency
- Clears errno on successful completion
- Part of the ECPG pgtypes library for type conversions between C types and PostgreSQL types
- The two-step conversion (double -> string -> numeric) ensures consistent parsing behavior with other numeric input methods

## Simplified Source

```c
int PGTYPESnumeric_from_double(double d, numeric *dst)
{
    char buffer[DBL_DIG + 100];
    numeric *tmp;
    int i;

    // Format double to string with full precision
    if (sprintf(buffer, "%.*g", DBL_DIG, d) <= 0)
        return -1;

    // Convert string to numeric
    if ((tmp = PGTYPESnumeric_from_asc(buffer, NULL)) == NULL)
        return -1;

    // Copy result to destination
    i = PGTYPESnumeric_copy(tmp, dst);
    PGTYPESnumeric_free(tmp);

    if (i != 0)
        return -1;

    errno = 0;
    return 0;
}
```