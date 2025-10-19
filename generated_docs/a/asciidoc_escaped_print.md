# asciidoc_escaped_print

## Location
[src/fe_utils/print.c:2168-2185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L2168-L2185)

## Overview
Escapes special AsciiDoc characters in a string for safe output to AsciiDoc format, specifically handling the pipe character which has special meaning in AsciiDoc tables.

## Definition

```c
static void
asciidoc_escaped_print(const char *in, FILE *fout)
```
## Detailed Description
This function processes an input string and escapes characters that have special meaning in AsciiDoc markup to prevent formatting conflicts and ensure proper display. Currently, it specifically handles the pipe character ('|') which is used as a table cell delimiter in AsciiDoc format. The function iterates through each character in the input string and either outputs the escaped version or the original character unchanged.

## Parameters / Member Variables
- `*in`: Input string to be AsciiDoc-escaped
- `*fout`: Output file stream where the escaped AsciiDoc text will be written
## Dependencies
- Functions called/Symbols referenced:
  - fputs (standard C library function)
  - fputc (standard C library function)
- Called from:
  - [print_asciidoc_text](../p/print_asciidoc_text.md) (src/fe_utils/print.c:2241, 2267)
  - [print_asciidoc_vertical](../p/print_asciidoc_vertical.md) (src/fe_utils/print.c:2354, 2361)

## Notes and Other Information
- Currently only escapes the pipe character ('|') by converting it to '\|'
- The pipe character is significant in AsciiDoc as it delimits table cells
- Much simpler than HTML escaping as AsciiDoc has fewer special characters that conflict with table formatting
- Essential for preventing AsciiDoc table formatting issues when displaying user data
- Used exclusively by PostgreSQL's AsciiDoc output formatting functions
- The escaped pipe prevents the character from being interpreted as a cell delimiter

## Simplified Source
```c
static void asciidoc_escaped_print(const char *in, FILE *fout)
{
    const char *p;

    // Process each character in the input string
    for (p = in; *p; p++) {
        if (*p == '|')
            fputs("\\|", fout);  // Escape pipe character for AsciiDoc tables
        else
            fputc(*p, fout);     // Output other characters unchanged
    }
}
```