# chr

## Location
[src/backend/utils/adt/oracle_compat.c:1006-1120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/oracle_compat.c#L1006-L1120)

## Overview
Converts an integer value to its corresponding character representation, supporting both ASCII and Unicode character sets depending on database encoding.

## Definition
```c
Datum chr(PG_FUNCTION_ARGS)
```

## Detailed Description
The chr function performs the inverse operation of the ascii function, converting a numeric value into its character representation. The function behavior depends on the database encoding: for UTF-8 databases, it treats the input as a Unicode code point and generates the appropriate multibyte UTF-8 sequence; for other multibyte encodings, it restricts input to ASCII range (1-127); for single-byte encodings, it accepts values 1-255. The function includes comprehensive validation to ensure the generated character is valid in the current database encoding, preventing invalid data from entering the database. It handles UTF-8 encoding with proper multibyte sequence generation for 2, 3, or 4-byte characters.

## Parameters / Member Variables
- `arg`: Integer value to be converted to a character (must be positive and non-zero)

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - PG_UTF8
  - SET_VARSIZE
  - VARDATA
  - [pg_utf8_islegal](../p/pg_utf8_islegal.md)
  - [pg_encoding_max_length](../p/pg_encoding_max_length.md)
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - Multiple references in regex engine (src/backend/regex/)
  - Color management functions in regex compiler
  - Character vector operations
  - Lexical analysis functions

## Notes and Other Information
- Located in src/backend/utils/adt/oracle_compat.c:1006-1120
- Part of PostgreSQL's Oracle compatibility layer
- Validates input parameters: rejects negative values and zero (null character)
- For UTF-8, implements full Unicode encoding up to U+10FFFF (RFC3629 limit)
- Generates proper 2, 3, or 4-byte UTF-8 sequences using bit manipulation
- Includes validation using pg_utf8_islegal to reject invalid sequences (e.g., surrogate pairs)
- For non-UTF-8 multibyte encodings, restricts to ASCII range for safety
- Ensures database encoding integrity by preventing invalid character data
- Memory allocation is optimized based on the expected output size

## Simplified Source

```c
// Simplified version of chr
Datum chr(PG_FUNCTION_ARGS) {
    int32 arg = PG_GETARG_INT32(0);
    uint32 cvalue;
    text *result;
    int encoding = GetDatabaseEncoding();

    // Basic validation: must be positive and non-zero
    if (arg <= 0) {
        ereport(ERROR, "character number must be positive");
    }

    cvalue = arg;

    // Handle UTF-8 encoding for Unicode characters
    if (encoding == PG_UTF8 && cvalue > 127) {
        // Check Unicode code point limit (U+10FFFF)
        if (cvalue > 0x0010ffff) {
            ereport(ERROR, "requested character too large");
        }

        // Determine UTF-8 byte length needed
        int bytes;
        if (cvalue > 0xffff)
            bytes = 4;      // 4-byte sequence
        else if (cvalue > 0x07ff)
            bytes = 3;      // 3-byte sequence
        else
            bytes = 2;      // 2-byte sequence

        // Allocate result and encode UTF-8 sequence
        result = (text *) palloc(VARHDRSZ + bytes);
        SET_VARSIZE(result, VARHDRSZ + bytes);
        unsigned char *wch = (unsigned char *) VARDATA(result);

        // Generate UTF-8 byte sequence based on length
        encode_utf8_sequence(wch, cvalue, bytes);

        // Validate the generated UTF-8 sequence
        if (!pg_utf8_islegal(wch, bytes)) {
            ereport(ERROR, "invalid character for encoding");
        }
    }
    else {
        // Handle ASCII and single-byte encodings
        bool is_multibyte = pg_encoding_max_length(encoding) > 1;

        // Check encoding limits
        if ((is_multibyte && cvalue > 127) || (!is_multibyte && cvalue > 255)) {
            ereport(ERROR, "character too large for encoding");
        }

        // Create single-byte result
        result = (text *) palloc(VARHDRSZ + 1);
        SET_VARSIZE(result, VARHDRSZ + 1);
        *VARDATA(result) = (char) cvalue;
    }

    PG_RETURN_TEXT_P(result);
}
```

Key simplifications made:
- Removed detailed error codes and messages for clarity
- Abstracted UTF-8 bit manipulation into conceptual `encode_utf8_sequence()` function
- Simplified conditional logic and variable names
- Consolidated similar validation patterns
- Focused on the main execution paths: UTF-8 multibyte vs single-byte handling
- Preserved essential algorithm: input validation, encoding detection, character generation, and output validation