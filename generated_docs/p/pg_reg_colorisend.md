# pg_reg_colorisend

## Location
[src/backend/regex/regexport.c:208-229](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexport.c#L208-L229)

## Overview
Checks whether a given color represents an end-of-line or end-of-string pseudo-character in a compiled regular expression.

## Definition

```c
struct cnfa *cnfa;
```
## Detailed Description
This function determines if a specified color code corresponds to an end-of-line (EOL) or end-of-string (EOS) pseudo-character in the regex engine. These are special "colors" that don't represent actual characters but instead represent positional assertions at the end of text.

The function examines the compiled NFA's  array, which contains two color values: one for end-of-string (EOS) and one for end-of-line (EOL). If the provided color matches either of these special colors, the function returns true.

## Parameters / Member Variables
- : Pointer to the compiled regular expression structure
- : Color number to check for end-of-line/string property

## Dependencies
- Functions called/Symbols referenced:
  - : Magic number constant for regex validation
  - : Internal regex structure containing the compiled NFA
  - : Compiled NFA structure containing positional color assignments
- Called from (representative examples):
  - External code that needs to distinguish positional pseudo-colors from regular character colors

## Notes and Other Information
- EOS (End of String) matches only at the very end of the input text
- EOL (End of Line) matches at the end of the input text or before any newline character
- These are "pseudo-colors" that represent positions rather than actual characters
- This function is the counterpart to , which checks for beginning-of-line/string colors
- Returns true (non-zero) if the color is an ending pseudo-color, false (0) otherwise
- Used to identify positional assertions during regex matching and analysis

## Simplified Source

```c
int pg_reg_colorisend(const regex_t *regex, int co) {
    // Validate input and get the compiled NFA
    struct cnfa *cnfa = &((struct guts *) regex->re_guts)->search;

    // Check if color matches end-of-string or end-of-line
    return (co == cnfa->eos[0] || co == cnfa->eos[1]);
}
```