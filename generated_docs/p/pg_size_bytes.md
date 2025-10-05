# pg_size_bytes

## Location
[src/backend/utils/adt/dbsize.c:713-878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L713-L878)

## Overview
This PostgreSQL SQL function converts a human-readable size string (with units like 'MB', 'GB') back into a numeric byte value as an int64.

## Definition
```c
Datum pg_size_bytes(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_size_bytes` function performs the reverse operation of `pg_size_pretty_numeric` by parsing a human-readable size string and converting it to bytes. The function implements a comprehensive parsing algorithm:

1. **Input Validation**: Extracts the input text and converts it to a C string
2. **Number Parsing**: Parses the numeric portion with support for:
   - Optional sign (+ or -)
   - Integer digits
   - Optional decimal point and fractional digits
   - Optional scientific notation exponent (e/E followed by digits)
3. **Unit Processing**: Identifies and validates the unit suffix by:
   - Searching through the standard `size_pretty_units` table
   - Checking unit aliases in `size_bytes_aliases` for alternative names
   - Performing case-insensitive matching
4. **Calculation**: Multiplies the numeric value by the appropriate unit multiplier using bit-shifting
5. **Result Conversion**: Converts the final Numeric result to int64 for return

The function handles whitespace trimming, validates input format, and provides detailed error messages for invalid inputs including unit validation hints.

## Parameters / Member Variables
- Function accepts one argument via `PG_GETARG_TEXT_PP(0)`: The text string containing the size with optional unit (e.g., '100 MB', '1.5GB', '512 bytes')

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP: Extracts text argument from function call
  - [text_to_cstring](../t/text_to_cstring.md): Converts PostgreSQL text to C string
  - isspace, isdigit: Character classification functions
  - DirectFunctionCall3: PostgreSQL function call interface for 3 parameters
  - [numeric_in](../n/numeric_in.md): Converts string to Numeric
  - [CStringGetDatum](../C/CStringGetDatum.md), ObjectIdGetDatum, Int32GetDatum: Datum conversion functions
  - [DatumGetNumeric](../D/DatumGetNumeric.md): Converts Datum to Numeric
  - size_pretty_units: Array of standard size unit definitions
  - size_bytes_aliases: Array of size unit aliases
  - [pg_strcasecmp](pg_strcasecmp.md): Case-insensitive string comparison
  - [int64_to_numeric](../i/int64_to_numeric.md): Converts int64 to Numeric
  - DirectFunctionCall2: PostgreSQL function call interface for 2 parameters  
  - [numeric_mul](../n/numeric_mul.md): Numeric multiplication
  - [NumericGetDatum](../N/NumericGetDatum.md): Converts Numeric to Datum
  - DirectFunctionCall1: PostgreSQL function call interface for 1 parameter
  - [numeric_int8](../n/numeric_int8.md): Converts Numeric to int64
  - [DatumGetInt64](../D/DatumGetInt64.md): Converts Datum to int64
  - PG_RETURN_INT64: Returns int64 result from function
- Called from (representative examples):
  - No direct references found (likely called via SQL)

## Notes and Other Information
This function is designed to be called from SQL as `pg_size_bytes(text)`. It provides robust parsing with comprehensive error handling and supports both standard units (bytes, kB, MB, GB, TB, PB) and their aliases. The function can handle fractional values and scientific notation. Input validation includes checking for valid numeric format and recognized units, with helpful error messages guiding users to correct formats. The function is the inverse of `pg_size_pretty` functions, enabling round-trip conversions between byte counts and human-readable representations. Located in src/backend/utils/adt/dbsize.c:713-878.

## Simplified Source

```c
Datum pg_size_bytes(PG_FUNCTION_ARGS) {
    text *arg = PG_GETARG_TEXT_PP(0);
    char *str = text_to_cstring(arg);
    char *strptr = str;
    char *endptr;
    Numeric num;
    int64 result;

    // Skip leading whitespace
    while (isspace((unsigned char) *strptr)) {
        strptr++;
    }

    // Parse the numeric part (sign, digits, decimal point, exponent)
    endptr = strptr;
    bool have_digits = false;

    // Parse sign, digits, decimal point, and optional exponent
    if (*endptr == '-' || *endptr == '+') endptr++;

    if (isdigit((unsigned char) *endptr)) {
        have_digits = true;
        while (isdigit((unsigned char) *endptr)) endptr++;
    }

    if (*endptr == '.') {
        endptr++;
        if (isdigit((unsigned char) *endptr)) {
            have_digits = true;
            while (isdigit((unsigned char) *endptr)) endptr++;
        }
    }

    if (!have_digits) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("invalid size: \"%s\"", str)));
    }

    // Handle optional scientific notation
    if (*endptr == 'e' || *endptr == 'E') {
        char *cp;
        strtol(endptr + 1, &cp, 10);
        if (cp > endptr + 1) endptr = cp;
    }

    // Convert the numeric part to Numeric
    char saved_char = *endptr;
    *endptr = '\0';
    num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
                                             CStringGetDatum(strptr),
                                             ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(-1)));
    *endptr = saved_char;

    // Skip whitespace before unit
    strptr = endptr;
    while (isspace((unsigned char) *strptr)) strptr++;

    // Handle unit if present
    if (*strptr != '\0') {
        const struct size_pretty_unit *unit = NULL;

        // Look for unit in standard units table
        for (const struct size_pretty_unit *u = size_pretty_units; u->name != NULL; u++) {
            if (pg_strcasecmp(strptr, u->name) == 0) {
                unit = u;
                break;
            }
        }

        // Look in aliases table if not found
        if (unit == NULL) {
            for (const struct size_bytes_unit_alias *a = size_bytes_aliases; a->alias != NULL; a++) {
                if (pg_strcasecmp(strptr, a->alias) == 0) {
                    unit = &size_pretty_units[a->unit_index];
                    break;
                }
            }
        }

        if (unit == NULL) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("invalid size: \"%s\"", text_to_cstring(arg))));
        }

        // Apply unit multiplier
        int64 multiplier = ((int64) 1) << unit->unitbits;
        if (multiplier > 1) {
            Numeric mul_num = int64_to_numeric(multiplier);
            num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
                                                     NumericGetDatum(mul_num),
                                                     NumericGetDatum(num)));
        }
    }

    // Convert to int64 and return
    result = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(num)));
    PG_RETURN_INT64(result);
}
```