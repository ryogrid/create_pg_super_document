# lexi

## Location
src/tools/pg_bsd_indent/lexi.c: 216 - 676

## Overview
The main lexical analyzer function for pg_bsd_indent that tokenizes C source code and returns appropriate token codes for the parser.

## Definition
int lexi(struct parser_state *state)

## Detailed Description
This is the central lexical analysis function that scans the input buffer and identifies tokens in C source code. It handles alphanumeric tokens (identifiers, keywords, numbers), string and character literals, operators, punctuation, and comments. The function maintains parser state information and determines whether identifiers are keywords, type names, or regular identifiers. It also performs lookahead analysis to distinguish between function definitions and declarations. The function returns integer token codes that guide the parsing and formatting process.

## Parameters / Member Variables
- state: Pointer to parser_state structure containing current parsing context, column position, keyword information, and various flags

## Dependencies
- Functions called/Symbols referenced:
  - [fill_buffer](../f/fill_buffer.md) (refills input buffer when needed)
  - [strcmp_type](../s/strcmp_type.md) (comparison function for binary search)
  - [is_func_definition](../i/is_func_definition.md) (determines if parentheses indicate function definition)
  - [diag2](../d/diag2.md) (diagnostic message output)
  - CHECK_SIZE_TOKEN (macro to ensure token buffer space)
- Called from (representative examples):
  - [main](../m/main.md) (at src/tools/pg_bsd_indent/indent.c:269)
  - [main](../m/main.md) (at src/tools/pg_bsd_indent/indent.c:448)

## Notes and Other Information
- Returns various token type codes: ident, decl, funcname, comment, newline, etc.
- Handles C numeric literals including binary (0b), octal (0), hexadecimal (0x), and decimal formats
- Recognizes C keywords through binary search in specials table
- Supports type name recognition through typename tables and _t suffix heuristics
- Manages string and character literal parsing with escape sequence handling  
- Tracks unary vs binary operator context through parser state
- Handles comment detection and parsing (both /* */ and // styles)
- Maintains column position information for formatting decisions
- Core component of the pg_bsd_indent code formatting tool