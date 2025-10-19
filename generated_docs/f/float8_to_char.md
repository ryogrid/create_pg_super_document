# float8_to_char

## Location
[src/backend/utils/adt/formatting.c:6829-6924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6829-L6924)

## Overview
Converts a double-precision floating-point number (float8) to its formatted text representation according to a specified format pattern.

## Definition

```c
Datum
float8_to_char(PG_FUNCTION_ARGS)
```
## Detailed Description
The `float8_to_char` function formats a double-precision floating-point value into a text string according to a format specification. It handles the unique characteristics of double-precision floating-point numbers including special values (NaN, infinity) and precision limitations. The function supports three main formatting categories:

1. **Roman numeral formatting**: Rounds the double to the nearest integer and converts to Roman numerals
2. **Scientific notation (EEEE format)**: Uses printf-style scientific notation with proper handling of special values
3. **Standard decimal formatting**: Manages floating-point precision limitations using DBL_DIG and adjusts decimal places accordingly

The function includes special handling for double-precision floating-point precision by limiting the total significant digits to DBL_DIG (typically 15-17 digits) to avoid displaying meaningless precision artifacts. This is similar to float4_to_char but with higher precision limits appropriate for double-precision values.

## Parameters / Member Variables
- `PG_GETARG_FLOAT8(0)`: The double-precision floating-point value to be formatted
- `PG_GETARG_TEXT_PP(1)`: The format pattern string specifying how the number should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8, PG_GETARG_TEXT_PP (PostgreSQL function argument macros)
  - float8, NUMDesc, FormatNode (data types and formatting structures)
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish (formatting preparation/cleanup macros)
  - IS_ROMAN, IS_EEEE, IS_MULTI (format type checking macros)
  - [int_to_roman](../i/int_to_roman.md) (Roman numeral conversion)
  - isnan, isinf (floating-point special value detection)
  - rint (rounding to nearest integer)
  - [psprintf](../p/psprintf.md) (PostgreSQL's sprintf wrapper)
  - [fill_str](fill_str.md) (string padding utility)
  - pow (power function for multiplier calculations)
  - fabs (absolute value function)
  - DBL_DIG (double-precision floating-point precision constant)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Special handling for NaN and infinity values in scientific notation by filling with '#' characters
- Precision management: limits total digits to DBL_DIG (typically 15-17 digits) to avoid displaying false precision
- Automatic adjustment of decimal places based on the number of digits before the decimal point
- Roman numeral conversion uses rounding to convert double to integer
- Scientific notation replaces leading '+' with space for alignment consistency
- Overflow conditions in standard formatting are handled by filling with '#' characters
- Higher precision handling compared to float4_to_char due to double-precision characteristics
- Part of PostgreSQL's comprehensive text formatting system in src/backend/utils/adt/formatting.c
- Uses double arithmetic for multiplier calculations to maintain precision

## Simplified Source

```c
Datum
float8_to_char(PG_FUNCTION_ARGS)
{
    float8 value = PG_GETARG_FLOAT8(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    NUMDesc Num;
    FormatNode *format;
    text *result;
    bool shouldFree;
    int out_pre_spaces = 0, sign = 0;
    char *numstr, *p;

    // Prepare format parsing and number description
    NUM_TOCHAR_prepare;

    if (IS_ROMAN(&Num))
    {
        // Convert to Roman numerals
        numstr = int_to_roman((int) rint(value));
    }
    else if (IS_EEEE(&Num))
    {
        // Scientific notation formatting
        if (isnan(value) || isinf(value))
        {
            // Handle special values (NaN/infinity) with '#' fill
            numstr = (char *) palloc(Num.pre + Num.post + 7);
            fill_str(numstr, '#', Num.pre + Num.post + 6);
            *numstr = ' ';
            *(numstr + Num.pre + 1) = '.';
        }
        else
        {
            // Format in scientific notation
            numstr = psprintf("%+.*e", Num.post, value);
            // Replace '+' with space for consistency
            if (*numstr == '+')
                *numstr = ' ';
        }
    }
    else
    {
        // Standard decimal formatting
        float8 val = value;
        char *orgnum;
        int numstr_pre_len;

        // Apply multiplier if specified
        if (IS_MULTI(&Num))
        {
            double multi = pow((double) 10, (double) Num.multi);
            val = value * multi;
            Num.pre += Num.multi;
        }

        // Calculate precision limits based on DBL_DIG
        orgnum = psprintf("%.0f", fabs(val));
        numstr_pre_len = strlen(orgnum);

        // Adjust precision to avoid false precision artifacts
        if (numstr_pre_len >= DBL_DIG)
            Num.post = 0;
        else if (numstr_pre_len + Num.post > DBL_DIG)
            Num.post = DBL_DIG - numstr_pre_len;

        orgnum = psprintf("%.*f", Num.post, val);

        // Extract sign and number part
        if (*orgnum == '-')
        {
            sign = '-';
            numstr = orgnum + 1;
        }
        else
        {
            sign = '+';
            numstr = orgnum;
        }

        // Calculate pre-decimal length and padding needs
        if ((p = strchr(numstr, '.')))
            numstr_pre_len = p - numstr;
        else
            numstr_pre_len = strlen(numstr);

        if (numstr_pre_len < Num.pre)
            out_pre_spaces = Num.pre - numstr_pre_len;
        else if (numstr_pre_len > Num.pre)
        {
            // Handle overflow with '#' fill
            numstr = (char *) palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
            *(numstr + Num.pre) = '.';
        }
    }

    // Complete formatting and return result
    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}
```