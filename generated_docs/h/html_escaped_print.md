# html_escaped_print

## Location
src/fe_utils/print.c: 1952 - 1992

## Overview
Escapes special HTML characters in a string for safe output to HTML format, handling characters that would otherwise be interpreted as HTML markup.

## Definition


## Detailed Description
This function converts special characters in an input string to their corresponding HTML entities to prevent HTML injection and ensure proper display in HTML contexts. It processes each character in the input string and outputs either the escaped HTML entity or the original character. The function also handles leading spaces specially by converting them to non-breaking spaces (&nbsp;) to preserve formatting, particularly useful for EXPLAIN output where indentation is significant.

## Parameters / Member Variables
- : Input string to be HTML-escaped
- : Output file stream where the escaped HTML will be written

## Dependencies
- Functions called/Symbols referenced:
  - fputs (standard C library function)
  - fputc (standard C library function)
- Called from:
  - PrintQueryStatus (src/bin/psql/common.c:978)
  - print_html_text (src/fe_utils/print.c:2015, 2026, 2048, 2070)
  - print_html_vertical (src/fe_utils/print.c:2105, 2126, 2134, 2151)

## Notes and Other Information
- Converts '&' to '&amp;', '<' to '&lt;', '>' to '&gt;', '"' to '&quot;'
- Newlines are converted to '<br />' HTML line breaks
- Leading spaces are converted to '&nbsp;' to preserve formatting in HTML output
- Subsequent spaces within a line are output as regular spaces
- Primarily used by PostgreSQL's HTML output formatting functions
- Essential for preventing HTML injection when displaying user data in HTML format