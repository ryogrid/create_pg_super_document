# numeric_to_number

## Location
[src/backend/utils/adt/formatting.c:6343-6401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6343-L6401)

## Overview
A PostgreSQL built-in function that converts a formatted text string to a numeric value using a specified format pattern.

## Definition

```c
Datum
numeric_to_number(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's TO_NUMBER functionality for converting formatted text strings into numeric values. It takes a text string containing a formatted number and a format pattern, then parses the string according to the pattern to extract the underlying numeric value.

The function works by:
1. Validating the format string length and allocating necessary resources
2. Using NUM_cache to parse and cache the format pattern 
3. Calling NUM_processor to perform the actual string-to-number conversion
4. Converting the resulting string to a Numeric type using numeric_in
5. Applying any scaling operations if multiplicative patterns (like 'V') are used
6. Cleaning up allocated resources

The function supports all standard PostgreSQL number formatting patterns including decimal places, thousands separators, currency symbols, signs, and scaling factors.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro, which provides access to:
  - : Input formatted text string to convert
  - : Format pattern string specifying expected format

## Dependencies
- Functions called/Symbols referenced:
  - [NUM_cache](../N/NUM_cache.md), NUM_processor, numeric_in, DirectFunctionCall3, DirectFunctionCall2
  - [int64_to_numeric](../i/int64_to_numeric.md), numeric_power, numeric_mul
  - [CStringGetDatum](../C/CStringGetDatum.md), NumericGetDatum, DatumGetNumeric, PG_GET_COLLATION
  - VARSIZE_ANY_EXHDR, VARDATA_ANY, palloc, pfree
  - IS_MULTI (macro for checking multiplicative formatting)
- Called from (representative examples):
  - This is a SQL-callable function, typically not called directly from C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as TO_NUMBER(text, text)
- Handles input validation including format string length limits
- Supports scaling operations for format patterns containing 'V' (multiplier/divisor)
- Uses format caching for performance optimization
- Returns NULL for invalid format specifications
- Memory management includes proper cleanup of allocated resources
- Works with PostgreSQL's collation system for locale-aware parsing
- The result precision and scale are derived from the format pattern
- Part of PostgreSQL's comprehensive formatting system in formatting.c

## Simplified Source

```c
Datum numeric_to_number(PG_FUNCTION_ARGS) {
    text *value = PG_GETARG_TEXT_PP(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    NUMDesc Num;
    Datum result;
    FormatNode *format;
    char *numstr;
    bool shouldFree;
    int len, scale, precision;

    // Validate format string length
    len = VARSIZE_ANY_EXHDR(fmt);
    if (len <= 0 || len >= INT_MAX / NUM_MAX_ITEM_SIZ)
        PG_RETURN_NULL();

    // Get parsed format from cache or parse new one
    format = NUM_cache(len, &Num, fmt, &shouldFree);

    // Allocate buffer for converted number string
    numstr = (char *) palloc((len * NUM_MAX_ITEM_SIZ) + 1);

    // Process input string according to format pattern
    NUM_processor(format, &Num, VARDATA_ANY(value), numstr,
                  VARSIZE_ANY_EXHDR(value), 0, 0, false, PG_GET_COLLATION());

    // Determine precision and scale from format
    scale = Num.post;
    precision = Num.pre + Num.multi + scale;

    // Clean up format if needed
    if (shouldFree)
        pfree(format);

    // Convert string to numeric with proper precision/scale
    result = DirectFunctionCall3(numeric_in,
                                CStringGetDatum(numstr),
                                ObjectIdGetDatum(InvalidOid),
                                Int32GetDatum(((precision << 16) | scale) + VARHDRSZ));

    // Apply scaling if multiplicative pattern was used
    if (IS_MULTI(&Num)) {
        Numeric x;
        Numeric a = int64_to_numeric(10);
        Numeric b = int64_to_numeric(-Num.multi);

        x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
                                               NumericGetDatum(a),
                                               NumericGetDatum(b)));
        result = DirectFunctionCall2(numeric_mul, result, NumericGetDatum(x));
    }

    pfree(numstr);
    return result;
}
```