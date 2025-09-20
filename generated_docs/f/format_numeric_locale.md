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

## Parameters / Member Variables
- : A null-terminated string representing a numeric value to be formatted with locale-specific rules

## Dependencies
- Functions called/Symbols referenced:
  - strspn (standard C library function)
  - strlen (standard C library function)
  - [pg_strdup](../p/pg_strdup.md)
  - [additional_numeric_locale_len](../a/additional_numeric_locale_len.md)
  - pg_malloc
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