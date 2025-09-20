# text_format_string_conversion

## Location
[src/backend/utils/adt/varlena.c:6041-6089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6041-L6089)

## Overview
Formats string conversions (%s, %I, or %L) in PostgreSQL's format string processing, handling string values, SQL identifiers, and SQL literals with proper escaping and formatting.

## Definition

```c
static void
text_format_string_conversion(StringInfo buf, char conversion,
							  FmgrInfo *typOutputInfo,
							  Datum value, bool isNull,
							  int flags, int width)
```
## Detailed Description
This function handles the formatting of string-like conversions in PostgreSQL's text formatting system. It supports three types of conversions:
- '%s': Regular string formatting
- '%I': SQL identifier formatting (with proper quoting)
- '%L': SQL literal formatting (with proper escaping)

The function first handles NULL values appropriately for each conversion type, then converts the input value to a string using the provided output function, applies the necessary escaping based on the conversion type, and finally appends the formatted string to the output buffer with the specified formatting flags and width.

## Parameters / Member Variables
- : StringInfo buffer where the formatted output is appended
- : Character indicating the conversion type ('s', 'I', or 'L')
- : Function manager info for the data type's output function
- : The Datum value to be formatted
- : Boolean indicating if the value is NULL
- : Formatting flags controlling alignment and padding
- : Field width for formatting

## Dependencies
- Functions called/Symbols referenced:
  - [text_format_append_string](text_format_append_string.md)
  - [OutputFunctionCall](../O/OutputFunctionCall.md)
  - [quote_identifier](../q/quote_identifier.md)
  - [quote_literal_cstr](../q/quote_literal_cstr.md)
  - [pfree](../p/pfree.md)
- Called from:
  - [text_format](text_format.md) (src/backend/utils/adt/varlena.c:5876)

## Notes and Other Information
- NULL handling varies by conversion type: '%s' converts to empty string, '%L' to "NULL", while '%I' raises an error since NULL cannot be formatted as an SQL identifier
- For '%I' conversions, quote_identifier is used to properly escape SQL identifiers
- For '%L' conversions, quote_literal_cstr is used to escape string literals for SQL
- Memory management is handled properly with pfree calls for allocated strings
- This function is part of PostgreSQL's format() SQL function implementation