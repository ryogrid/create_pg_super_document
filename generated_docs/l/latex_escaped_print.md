# latex_escaped_print

## Location
[src/fe_utils/print.c:2392-2453](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2392-L2453)

## Overview
Escapes special LaTeX characters in text strings to ensure they are properly rendered in LaTeX documents without causing syntax errors or formatting issues.

## Definition

```c
static void
latex_escaped_print(const char *in, FILE *fout)
```
## Detailed Description
This function processes input text character by character and converts special LaTeX metacharacters to their escaped equivalents. It follows recommendations from Scott Pakin's "The Comprehensive LATEX Symbol List" for ASCII character conversions. The function handles characters that have special meaning in LaTeX (like #, $, %, &, etc.) by prefixing them with backslashes or replacing them with appropriate LaTeX commands. For non-ASCII characters, no special handling is performed.

## Parameters / Member Variables
- : Input string containing the text to be escaped for LaTeX output
- : File stream where the escaped LaTeX text will be written

## Dependencies
- Functions called/Symbols referenced:
  - fputs (for outputting escaped character sequences)
  - fputc (for outputting regular characters)
- Called from (representative examples):
  - [print_latex_text](../p/print_latex_text.md) (main LaTeX table printing function)
  - [print_latex_vertical](../p/print_latex_vertical.md) (vertical LaTeX table format)
  - LONGTABLE_WHITESPACE (macro for longtable formatting)

## Notes and Other Information
- This is a static function within print.c, used internally for LaTeX formatting
- Handles 13 special LaTeX characters: # $ % & < > \ ^ _ { | } ~
- Uses specific LaTeX commands for some characters (e.g., \textless{} for <, \textbar{} for |)
- Newline characters are converted to LaTeX line breaks (\\\\), though the comment notes this approach is imperfect
- Non-ASCII characters pass through unchanged - users must handle Unicode separately
- Based on widely-used LaTeX symbol reference documentation for compatibility