# PGTYPESnumeric_to_int

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1494-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1494-L1517)

## Overview
Converts a PostgreSQL numeric value to a C integer type, with overflow checking for platforms where long and int have different sizes.

## Definition
```c
int PGTYPESnumeric_to_int(numeric *nv, int *ip)
```

## Detailed Description
This function converts a PostgreSQL numeric value to a C int. It internally delegates to `PGTYPESnumeric_to_long` and then performs range checking to ensure the long value fits within the range of an int. On platforms where sizeof(long) > sizeof(int), it explicitly checks for overflow conditions and returns an error if the value exceeds INT_MIN or INT_MAX.

The function follows the ECPG (Embedded SQL in C) pattern of returning 0 on success and non-zero error codes on failure, with the converted value stored in the output parameter.

## Parameters / Member Variables
- `nv`: Input numeric value to convert (pointer to numeric structure)
- `ip`: Output parameter to store the converted integer value (pointer to int)

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_to_long](PGTYPESnumeric_to_long.md)
  - PGTYPES_NUM_OVERFLOW (error constant)
  - [numeric](../n/numeric.md) (type)
- Called from (representative examples):
  - [dectoint](../d/dectoint.md) (in compatlib/informix.c)
  - [main](../m/main.md) (in test files for numeric operations)

## Notes and Other Information
- Returns 0 on successful conversion, non-zero on error
- On overflow, sets errno to PGTYPES_NUM_OVERFLOW and returns -1
- The overflow check is conditionally compiled based on SIZEOF_LONG vs SIZEOF_INT
- Part of the ECPG pgtypes library for embedded SQL applications
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:1494-1517

## Simplified Source

```c
int PGTYPESnumeric_to_int(numeric *nv, int *ip)
{
    long l;
    int i;

    // First convert to long
    if ((i = PGTYPESnumeric_to_long(nv, &l)) != 0)
        return i;

    // Check for overflow on platforms where long > int
#if SIZEOF_LONG > SIZEOF_INT
    if (l < INT_MIN || l > INT_MAX) {
        errno = PGTYPES_NUM_OVERFLOW;
        return -1;
    }
#endif

    // Cast to int and store result
    *ip = (int) l;
    return 0;
}
```