# text_format_append_string

## Location
[src/backend/utils/adt/varlena.c:6090-6141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L6090-L6141)

## Overview
Appends a string to a StringInfo buffer with optional padding and alignment based on specified flags and field width in PostgreSQL's text formatting system.

## Definition

```c
static void
text_format_append_string(StringInfo buf, const char *str,
						  int flags, int width)
```
## Detailed Description
This function handles string appending with formatting capabilities including field width control and alignment. It supports both left and right justification through flags or negative width values. When a field width is specified, the function pads the string with spaces to meet the required width. The function uses multibyte-aware string length calculation to properly handle Unicode characters.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the formatted string will be appended
- `*str`: The input string to be formatted and appended
- `flags`: Formatting flags (TEXT_FORMAT_FLAG_MINUS for left alignment)
- `width`: Field width for formatting (negative values imply left alignment)
## Dependencies
- Functions called/Symbols referenced:
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoSpaces](../a/appendStringInfoSpaces.md)
  - [pg_mbstrlen](../p/pg_mbstrlen.md)
  - TEXT_FORMAT_FLAG_MINUS (constant)
- Called from:
  - [text_format_string_conversion](text_format_string_conversion.md) (multiple calls at lines 6052, 6054, 6069, 6075, 6080)

## Notes and Other Information
- Fast path optimization when width is 0 - simply appends the string without formatting
- Negative width values automatically enable left alignment and are converted to absolute values
- Uses pg_mbstrlen for multibyte character support to calculate proper string length
- Left justification: string first, then padding spaces
- Right justification: padding spaces first, then string
- Includes safety check for INT_MIN overflow when converting negative width to positive
- This function is a key component of PostgreSQL's format() function implementation

## Simplified Source

```c
static void text_format_append_string(StringInfo buf, const char *str,
                                     int flags, int width) {
    bool align_to_left = false;
    int len;

    // Fast path: no width formatting needed
    if (width == 0) {
        appendStringInfoString(buf, str);
        return;
    }

    // Handle negative width (implies left alignment)
    if (width < 0) {
        align_to_left = true;
        if (width <= INT_MIN)
            ereport(ERROR, "number is out of range");
        width = -width;
    } else if (flags & TEXT_FORMAT_FLAG_MINUS) {
        align_to_left = true;
    }

    // Get multibyte-aware string length
    len = pg_mbstrlen(str);

    if (align_to_left) {
        // Left justify: string first, then padding
        appendStringInfoString(buf, str);
        if (len < width)
            appendStringInfoSpaces(buf, width - len);
    } else {
        // Right justify: padding first, then string
        if (len < width)
            appendStringInfoSpaces(buf, width - len);
        appendStringInfoString(buf, str);
    }
}
```