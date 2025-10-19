# intervaltypmodout

## Location
[src/backend/utils/adt/timestamp.c:1135-1220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1135-L1220)

## Overview
Converts internal INTERVAL type modifier values back into human-readable string representations, decoding the compressed typmod format into field specifications and precision information.

## Definition
```c
Datum intervaltypmodout(PG_FUNCTION_ARGS)
```

## Detailed Description
The `intervaltypmodout` function performs the reverse operation of `intervaltypmodin`, taking a compressed 32-bit typmod value and converting it back into a readable string format that describes the interval type specification. This function is used by PostgreSQL's type system to display interval type information in error messages, catalog views, and type descriptions.

The function decodes the typmod by extracting:
1. **Range information** from the high 16 bits using INTERVAL_RANGE()
2. **Precision information** from the low 16 bits using INTERVAL_PRECISION()

It then translates these into standard SQL interval notation (e.g., "day to second(3)", "year to month", "hour(2)").

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `typmod`: 32-bit integer containing encoded interval type modifier
  - `res`: Allocated string buffer (64 bytes) for result
  - `fields`: Decoded field range bitmap
  - `precision`: Decoded precision value
  - `fieldstr`: String representation of field range

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
  - [palloc](../p/palloc.md)
  - INTERVAL_RANGE
  - INTERVAL_PRECISION
  - INTERVAL_MASK
  - INTERVAL_FULL_RANGE
  - INTERVAL_FULL_PRECISION
  - snprintf
  - PG_RETURN_CSTRING
  - elog (for error reporting)
- Called from (representative examples):
  - SQL type system for displaying interval type information
  - Error reporting and catalog functions
  - Type description utilities

## Notes and Other Information
- Returns empty string for negative typmod values (indicating no type modifier)
- Handles all standard SQL interval field combinations (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND)
- Supports compound ranges like "day to second" and "year to month"
- Only includes precision specification in output when it differs from INTERVAL_FULL_PRECISION
- Uses a fixed 64-byte buffer for output formatting, sufficient for all valid interval type specifications
- Part of PostgreSQL's type system infrastructure providing human-readable type information

## Simplified Source

```c
Datum
intervaltypmodout(PG_FUNCTION_ARGS)
{
    int32 typmod = PG_GETARG_INT32(0);
    char *res = (char *) palloc(64);
    int fields;
    int precision;
    const char *fieldstr;

    // Return empty string for invalid typmod
    if (typmod < 0) {
        *res = '\0';
        PG_RETURN_CSTRING(res);
    }

    // Extract fields and precision from typmod
    fields = INTERVAL_RANGE(typmod);
    precision = INTERVAL_PRECISION(typmod);

    // Map field combinations to readable strings
    switch (fields) {
        case INTERVAL_MASK(YEAR):
            fieldstr = " year";
            break;
        case INTERVAL_MASK(MONTH):
            fieldstr = " month";
            break;
        case INTERVAL_MASK(DAY):
            fieldstr = " day";
            break;
        case INTERVAL_MASK(HOUR):
            fieldstr = " hour";
            break;
        case INTERVAL_MASK(MINUTE):
            fieldstr = " minute";
            break;
        case INTERVAL_MASK(SECOND):
            fieldstr = " second";
            break;
        case INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH):
            fieldstr = " year to month";
            break;
        case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR):
            fieldstr = " day to hour";
            break;
        case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
            fieldstr = " day to minute";
            break;
        case INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
            fieldstr = " day to second";
            break;
        case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE):
            fieldstr = " hour to minute";
            break;
        case INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
            fieldstr = " hour to second";
            break;
        case INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND):
            fieldstr = " minute to second";
            break;
        case INTERVAL_FULL_RANGE:
            fieldstr = "";
            break;
        default:
            elog(ERROR, "invalid INTERVAL typmod: 0x%x", typmod);
            fieldstr = "";
            break;
    }

    // Format result with or without precision
    if (precision != INTERVAL_FULL_PRECISION)
        snprintf(res, 64, "%s(%d)", fieldstr, precision);
    else
        snprintf(res, 64, "%s", fieldstr);

    PG_RETURN_CSTRING(res);
}
```