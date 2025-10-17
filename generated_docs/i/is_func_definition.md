# is_func_definition

## Location
[src/tools/pg_bsd_indent/lexi.c:160-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/lexi.c#L160-L215)

## Overview
A static function that distinguishes between C function definitions and function declarations by analyzing ahead in the input stream to determine the syntactic context.

## Definition
static int is_func_definition(char *tp)

## Detailed Description
This function implements a lookahead parser to determine whether a parenthesis-enclosed parameter list belongs to a function definition (followed by {) or a function declaration (followed by ; or ,). It handles nested parentheses, C-style comments (/* */), C++-style comments (//), and looks beyond the current buffer if necessary. The algorithm scans forward from the given position, maintaining state for comment parsing and parenthesis depth, until it finds a definitive character that indicates the functions purpose.

## Parameters / Member Variables
- tp: Pointer to the current position in the input buffer, typically pointing to an opening parenthesis

## Dependencies
- Functions called/Symbols referenced:
  - [lookahead_reset](../l/lookahead_reset.md) (resets the lookahead buffer)
  - [lookahead](../l/lookahead.md) (reads characters beyond the current buffer)
- Called from (representative examples):
  - [lexi](../l/lexi.md) (at src/tools/pg_bsd_indent/lexi.c:415)

## Notes and Other Information
- Returns true for function definitions, false for declarations
- Can look past the end of the current buffer using lookahead mechanisms  
- Handles comment parsing to avoid being confused by syntax within comments
- Tracks parenthesis depth to ensure proper nesting analysis
- May be fooled by K&R-style parameter declarations but this is considered acceptable
- Could potentially be confused by mismatched parentheses or comment-like patterns in string literals
- Returns false on EOF or unbalanced parentheses (assumes declaration)

## Simplified Source

```c
static int is_func_definition(char *tp) {
    int paren_depth = 0;
    int in_comment = false;
    int in_slash_comment = false;
    int lastc = 0;

    // Look ahead past current buffer if needed
    lookahead_reset();

    for (;;) {
        int c;

        // Get next character from buffer or lookahead
        if (tp < buf_end) {
            c = *tp++;
        } else {
            c = lookahead();
            if (c == EOF) break;
        }

        // Handle C-style comments /* */
        if (in_comment) {
            if (lastc == '*' && c == '/') {
                in_comment = false;
            }
        } else if (lastc == '/' && c == '*' && !in_slash_comment) {
            in_comment = true;
        }
        // Handle C++-style comments //
        else if (in_slash_comment) {
            if (c == '\n') {
                in_slash_comment = false;
            }
        } else if (lastc == '/' && c == '/') {
            in_slash_comment = true;
        }
        // Track parenthesis nesting
        else if (c == '(') {
            paren_depth++;
        } else if (c == ')') {
            paren_depth--;
            if (paren_depth < 0) {
                return false;  // Unbalanced parens = declaration
            }
        }
        // Check for definition/declaration indicators outside parens
        else if (paren_depth == 0) {
            if (c == '{') {
                return true;   // Function definition
            } else if (c == ';' || c == ',') {
                return false;  // Function declaration
            }
        }

        lastc = c;
    }

    return false;  // EOF reached = not a definition
}
```