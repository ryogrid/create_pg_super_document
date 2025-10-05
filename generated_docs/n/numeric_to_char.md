# numeric_to_char

## Location
[src/backend/utils/adt/formatting.c:6402-6526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6402-L6526)

## Overview
A PostgreSQL built-in function that converts a numeric value to a formatted text string using a specified format pattern.

## Definition

```c
Datum
numeric_to_char(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's TO_CHAR functionality for converting numeric values into formatted text strings. It takes a Numeric value and a format pattern, then produces a formatted string representation according to the pattern specifications.

The function handles several special formatting modes:
- Roman numeral conversion (RN/rn): Rounds the numeric value to an integer and converts it to Roman numerals
- Scientific notation (EEEE): Uses numeric_out_sci to format with exponential notation, handling special cases like NaN, Infinity, and -Infinity
- Standard formatting: Applies multiplicative scaling if specified, rounds according to format precision, and handles overflow conditions

The function manages sign handling, decimal alignment, padding with spaces or '#' characters for overflow, and proper formatting of special numeric values. It integrates with PostgreSQL's comprehensive formatting system using NUM_TOCHAR_prepare and NUM_TOCHAR_finish macros.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro, which provides access to:
  - : Input numeric value to format
  - : Format pattern string specifying desired output format

## Dependencies
- Functions called/Symbols referenced:
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish, numeric_round, numeric_out_sci
  - [int_to_roman](../i/int_to_roman.md), numeric_int4, numeric_out, numeric_power, numeric_mul
  - [int64_to_numeric](../i/int64_to_numeric.md), fill_str, DirectFunctionCall1, DirectFunctionCall2
  - [DatumGetNumeric](../D/DatumGetNumeric.md), NumericGetDatum, DatumGetInt32, DatumGetCString
  - IS_ROMAN, IS_EEEE, IS_MULTI (formatting condition macros)
- Called from (representative examples):
  - This is a SQL-callable function, typically not called directly from C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as TO_CHAR(numeric, text)
- Handles special numeric values (NaN, Infinity, -Infinity) with appropriate formatting
- Roman numeral conversion supports both uppercase (RN) and lowercase (rn) variants
- Scientific notation includes proper sign alignment for positive numbers
- Overflow conditions are handled by filling the output with '#' characters
- Supports multiplicative formatting patterns (V) that scale the input value
- Memory management includes proper allocation for output buffers of varying sizes
- Integrates with PostgreSQL's locale-aware number formatting system
- Returns a PostgreSQL text datum that can be used in SQL contexts
- Part of PostgreSQL's comprehensive formatting system in formatting.c

## Simplified Source

```c
Datum
numeric_to_char(PG_FUNCTION_ARGS)
{
    Numeric value = PG_GETARG_NUMERIC(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    NUMDesc Num;
    FormatNode *format;
    text *result;
    int out_pre_spaces = 0, sign = 0;
    char *numstr, *orgnum, *p;
    Numeric x;

    // Parse format pattern and initialize formatting structures
    NUM_TOCHAR_prepare;

    // Handle Roman numeral formatting
    if (IS_ROMAN(&Num)) {
        // Round to integer and convert to Roman numerals
        x = DatumGetNumeric(DirectFunctionCall2(numeric_round,
                                              NumericGetDatum(value),
                                              Int32GetDatum(0)));
        numstr = int_to_roman(DatumGetInt32(DirectFunctionCall1(numeric_int4,
                                                               NumericGetDatum(x))));
    }
    // Handle scientific notation formatting
    else if (IS_EEEE(&Num)) {
        orgnum = numeric_out_sci(value, Num.post);

        // Handle special values (NaN, Infinity)
        if (strcmp(orgnum, "NaN") == 0 || strcmp(orgnum, "Infinity") == 0 ||
            strcmp(orgnum, "-Infinity") == 0) {
            // Create padded output with '#' characters
            numstr = (char *) palloc(Num.pre + Num.post + 7);
            fill_str(numstr, '#', Num.pre + Num.post + 6);
            *numstr = ' ';
            *(numstr + Num.pre + 1) = '.';
        }
        // Add leading space for positive numbers for alignment
        else if (*orgnum != '-') {
            numstr = (char *) palloc(strlen(orgnum) + 2);
            *numstr = ' ';
            strcpy(numstr + 1, orgnum);
        }
        else {
            numstr = orgnum;
        }
    }
    // Handle standard numeric formatting
    else {
        Numeric val = value;

        // Apply multiplicative scaling if specified
        if (IS_MULTI(&Num)) {
            Numeric a = int64_to_numeric(10);
            Numeric b = int64_to_numeric(Num.multi);
            x = DatumGetNumeric(DirectFunctionCall2(numeric_power,
                                                  NumericGetDatum(a),
                                                  NumericGetDatum(b)));
            val = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
                                                    NumericGetDatum(value),
                                                    NumericGetDatum(x)));
            Num.pre += Num.multi;
        }

        // Round to specified precision and convert to string
        x = DatumGetNumeric(DirectFunctionCall2(numeric_round,
                                              NumericGetDatum(val),
                                              Int32GetDatum(Num.post)));
        orgnum = DatumGetCString(DirectFunctionCall1(numeric_out,
                                                   NumericGetDatum(x)));

        // Extract sign and number parts
        if (*orgnum == '-') {
            sign = '-';
            numstr = orgnum + 1;
        } else {
            sign = '+';
            numstr = orgnum;
        }

        // Calculate padding and handle overflow
        int numstr_pre_len;
        if ((p = strchr(numstr, '.')))
            numstr_pre_len = p - numstr;
        else
            numstr_pre_len = strlen(numstr);

        // Determine if padding needed or overflow occurred
        if (numstr_pre_len < Num.pre)
            out_pre_spaces = Num.pre - numstr_pre_len;
        else if (numstr_pre_len > Num.pre) {
            // Create overflow output with '#' characters
            numstr = (char *) palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
            *(numstr + Num.pre) = '.';
        }
    }

    // Apply formatting and return result
    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}
```