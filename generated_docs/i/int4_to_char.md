# int4_to_char

## Location
[src/backend/utils/adt/formatting.c:6527-6620](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L6527-L6620)

## Overview
A PostgreSQL built-in function that converts a 32-bit integer value to a formatted text string using a specified format pattern.

## Definition

```c
Datum
int4_to_char(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements PostgreSQL's TO_CHAR functionality specifically for 32-bit integer values. It takes an int32 value and a format pattern, then produces a formatted string representation according to the pattern specifications.

The function handles several formatting modes:
- Roman numeral conversion (RN/rn): Converts the integer directly to Roman numerals using int_to_roman
- Scientific notation (EEEE): Uses psprintf to format in exponential notation, converting to float8 for precision, and replaces leading '+' with space for alignment
- Standard formatting: Applies multiplicative scaling using integer arithmetic and pow(), handles decimal padding with zeros, and manages overflow conditions

Key features include sign extraction and handling, prefix padding calculation, overflow protection by filling with '#' characters, and post-decimal zero padding when decimal places are specified in the format.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro, which provides access to:
  - : Input 32-bit integer value to format
  - : Format pattern string specifying desired output format

## Dependencies
- Functions called/Symbols referenced:
  - NUM_TOCHAR_prepare, NUM_TOCHAR_finish, int_to_roman, int4out
  - DirectFunctionCall1, DatumGetCString, Int32GetDatum
  - [psprintf](../p/psprintf.md), fill_str, pow (mathematical power function)
  - IS_ROMAN, IS_EEEE, IS_MULTI (formatting condition macros)
- Called from (representative examples):
  - This is a SQL-callable function, typically not called directly from C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as TO_CHAR(integer, text)
- Optimized for integer values, avoiding floating-point precision issues where possible
- Roman numeral conversion works directly with the integer value without rounding
- Scientific notation temporarily converts to float8 but this won't lose precision for int32 range
- Multiplicative formatting (V pattern) uses integer arithmetic with pow() for scaling
- Handles post-decimal formatting by padding with zeros rather than actual decimal computation
- Overflow conditions are indicated by filling output with '#' characters
- Memory management includes proper allocation for various output buffer sizes
- Integrates with PostgreSQL's comprehensive formatting system using shared macros
- Returns a PostgreSQL text datum suitable for SQL result sets
- Part of PostgreSQL's family of type-specific formatting functions in formatting.c

## Simplified Source

```c
Datum
int4_to_char(PG_FUNCTION_ARGS)
{
    int32 value = PG_GETARG_INT32(0);
    text *fmt = PG_GETARG_TEXT_PP(1);
    NUMDesc Num;
    FormatNode *format;
    text *result;
    int out_pre_spaces = 0, sign = 0;
    char *numstr, *orgnum;

    // Parse format pattern and initialize formatting structures
    NUM_TOCHAR_prepare;

    // Handle Roman numeral formatting
    if (IS_ROMAN(&Num)) {
        numstr = int_to_roman(value);
    }
    // Handle scientific notation formatting
    else if (IS_EEEE(&Num)) {
        // Convert to float8 for exponential notation (no precision loss for int32)
        float8 val = (float8) value;
        orgnum = (char *) psprintf("%+.*e", Num.post, val);

        // Replace leading '+' with space for alignment
        if (*orgnum == '+')
            *orgnum = ' ';

        numstr = orgnum;
    }
    // Handle standard numeric formatting
    else {
        // Apply multiplicative scaling if specified
        if (IS_MULTI(&Num)) {
            orgnum = DatumGetCString(DirectFunctionCall1(int4out,
                Int32GetDatum(value * ((int32) pow((double) 10, (double) Num.multi)))));
            Num.pre += Num.multi;
        } else {
            orgnum = DatumGetCString(DirectFunctionCall1(int4out,
                                                       Int32GetDatum(value)));
        }

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