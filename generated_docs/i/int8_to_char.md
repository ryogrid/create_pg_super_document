# int8_to_char

## Location
[src/backend/utils/adt/formatting.c:6621-6726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6621-L6726)

## Overview
Converts a 64-bit integer (int8) to its formatted text representation according to a specified format pattern.

## Definition

```c
Datum
int8_to_char(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function formats a 64-bit integer value into a text string according to a format specification. It supports various formatting options including Roman numerals, scientific notation, decimal formatting with padding, and precision control. The function handles three main formatting categories:

1. **Roman numeral formatting**: Converts the int8 to int4 and then to Roman numerals (with precision limitations)
2. **Scientific notation (EEEE format)**: Uses numeric representation to maintain precision and avoid floating-point conversion
3. **Standard decimal formatting**: Supports padding, decimal places, sign handling, and overflow indication

The function uses PostgreSQL's standard formatting infrastructure with NUMDesc and FormatNode structures to parse and apply the format specification.

## Parameters / Member Variables
- : The 64-bit integer value to be formatted
- : The format pattern string specifying how the number should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64, PG_GETARG_TEXT_PP (PostgreSQL function argument macros)
  - NUMDesc, FormatNode (formatting structure types)
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish (formatting preparation/cleanup macros)
  - IS_ROMAN, IS_EEEE, IS_MULTI (format type checking macros)
  - [int_to_roman](int_to_roman.md) (Roman numeral conversion)
  - [int84](int84.md), int8out, int8mul (int8 type conversion functions)
  - [numeric_out_sci](../n/numeric_out_sci.md), int64_to_numeric (numeric precision handling)
  - DirectFunctionCall1, DirectFunctionCall2 (PostgreSQL function call utilities)
  - [fill_str](../f/fill_str.md) (string padding utility)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Roman numeral conversion is limited by the int84 conversion, potentially losing precision for very large int8 values
- Scientific notation formatting preserves full precision by avoiding floating-point conversion and using the numeric type instead
- The function handles sign display explicitly, adding space padding for positive numbers in scientific notation to maintain alignment
- Overflow conditions are handled by filling the output with '#' characters when the number exceeds the specified format width
- Part of PostgreSQL's comprehensive text formatting system in src/backend/utils/adt/formatting.c

## Simplified Source

```c
Datum
int8_to_char(PG_FUNCTION_ARGS)
{
    int64 value = PG_GETARG_INT64(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    NUMDesc Num;
    FormatNode *format;
    text *result;
    int out_pre_spaces = 0, sign = 0;
    char *numstr, *orgnum;

    // Parse format pattern and initialize formatting structures
    NUM_TOCHAR_prepare;

    // Handle Roman numeral formatting (limited to int4 range)
    if (IS_ROMAN(&Num)) {
        // Convert int8 to int4 first, then to Roman numerals
        numstr = int_to_roman(DatumGetInt32(DirectFunctionCall1(int84, Int64GetDatum(value))));
    }
    // Handle scientific notation formatting
    else if (IS_EEEE(&Num)) {
        // Use numeric to preserve precision (avoid float8 conversion)
        orgnum = numeric_out_sci(int64_to_numeric(value), Num.post);

        // Add leading space for positive numbers for alignment
        if (*orgnum != '-') {
            numstr = (char *) palloc(strlen(orgnum) + 2);
            *numstr = ' ';
            strcpy(numstr + 1, orgnum);
        } else {
            numstr = orgnum;
        }
    }
    // Handle standard numeric formatting
    else {
        // Apply multiplicative scaling if specified
        if (IS_MULTI(&Num)) {
            double multi = pow((double) 10, (double) Num.multi);
            value = DatumGetInt64(DirectFunctionCall2(int8mul,
                                                    Int64GetDatum(value),
                                                    DirectFunctionCall1(dtoi8,
                                                                       Float8GetDatum(multi))));
            Num.pre += Num.multi;
        }

        // Convert to string representation
        orgnum = DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(value)));

        // Extract sign
        if (*orgnum == '-') {
            sign = '-';
            orgnum++;
        } else {
            sign = '+';
        }

        int numstr_pre_len = strlen(orgnum);

        // Add decimal places if specified (pad with zeros)
        if (Num.post) {
            numstr = (char *) palloc(numstr_pre_len + Num.post + 2);
            strcpy(numstr, orgnum);
            *(numstr + numstr_pre_len) = '.';
            memset(numstr + numstr_pre_len + 1, '0', Num.post);
            *(numstr + numstr_pre_len + Num.post + 1) = '\0';
        } else {
            numstr = orgnum;
        }

        // Calculate padding and handle overflow
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