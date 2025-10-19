# format_numeric_locale

## Location
[src/fe_utils/print.c:314-378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L314-L378)

## Overview
Formats a numeric string according to the current LC_NUMERIC locale setting, adding thousands separators and locale-specific decimal points.

## Definition

```c
struct separator sep, FILE *fout)
{
	if (sep.separator_zero)
		fputc('\000', fout);
	else if (sep.separator)
		fputs(sep.separator, fout);
}


/*
 * Return the list of explicitly-requested footers or, when applicable, the
 * default "(xx rows)" footer.  Always omit the default footer when given
 * non-default footers, "\pset footer off", or a specific instruction to that
 * effect from a calling backslash command.  Vertical formats number each row,
 * making the default footer redundant;
```
## Detailed Description
The  function transforms a numeric string to conform to locale-specific formatting rules. It first validates that the input string contains only numeric characters (digits, signs, decimal point, and scientific notation). The function then allocates a new string with sufficient space for locale formatting, processes the sign character, inserts thousands separators at appropriate positions in the integral part, replaces the decimal point with the locale-specific decimal point, and copies any remaining fractional digits or exponent notation. The function requires that  has been called previously to initialize the locale-specific formatting variables.

## Parameters
- `my_str`: A null-terminated string representing a numeric value to be formatted with locale-specific rules

## Dependencies
- Functions called/Symbols referenced:
  - strspn (standard C library function)
  - strlen (standard C library function)
  - [pg_strdup](../p/pg_strdup.md)
  - [additional_numeric_locale_len](../a/additional_numeric_locale_len.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [integer_digits](../i/integer_digits.md)
  - strcpy (standard C library function)
  - Assert (macro)
- Global variables referenced:
  - groupdigits
  - thousands_sep
  - decimal_point
- Called from (representative examples):
  - [printQuery](../p/printQuery.md)

## Notes and Other Information
- This is a static function, only accessible within src/fe_utils/print.c
- Returns a newly allocated string that the caller must free
- Performs validation to avoid mangling already-localized "money" values by checking that the string contains only valid numeric characters
- Uses a sophisticated algorithm to insert thousands separators by tracking leading digits in each group
- Handles both positive and negative numbers by preserving the sign character
- Supports scientific notation (e/E) in the input string
- The function over-estimates the required buffer size for safety and uses Assert to verify the final string length
- Requires prior call to setDecimalLocale() to initialize locale variables

## Simplified Source

```c
static char *
format_numeric_locale(const char *my_str)
{
    char *new_str;
    int new_len, int_len, leading_digits, i, new_str_pos;

    // Validate input is numeric (digits, signs, decimal, exponent)
    if (strspn(my_str, "0123456789+-.eE") != strlen(my_str))
        return pg_strdup(my_str); // Return unchanged if not numeric

    // Calculate required space and allocate
    new_len = strlen(my_str) + additional_numeric_locale_len(my_str);
    new_str = pg_malloc(new_len + 1);
    new_str_pos = 0;
    int_len = integer_digits(my_str);

    // Calculate leading digits in first thousands group
    leading_digits = int_len % groupdigits;
    if (leading_digits == 0)
        leading_digits = groupdigits;

    // Copy sign if present
    if (my_str[0] == '-' || my_str[0] == '+')
    {
        new_str[new_str_pos++] = my_str[0];
        my_str++;
    }

    // Process integer part with thousands separators
    for (i = 0; i < int_len; i++)
    {
        // Insert thousands separator when needed
        if (i > 0 && --leading_digits == 0)
        {
            strcpy(&new_str[new_str_pos], thousands_sep);
            new_str_pos += strlen(thousands_sep);
            leading_digits = groupdigits;
        }
        new_str[new_str_pos++] = my_str[i];
    }

    // Replace decimal point with locale-specific version
    if (my_str[i] == '.')
    {
        strcpy(&new_str[new_str_pos], decimal_point);
        new_str_pos += strlen(decimal_point);
        i++;
    }

    // Copy remaining fractional/exponent part
    strcpy(&new_str[new_str_pos], &my_str[i]);

    Assert(strlen(new_str) <= new_len);
    return new_str;
}
```