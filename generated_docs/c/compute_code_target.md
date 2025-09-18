# compute_code_target

## Location
[src/tools/pg_bsd_indent/io.c:223-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L223-L251)

## Overview
Calculates the target column position for code indentation based on nesting level, parentheses alignment, and continuation settings.

## Definition


## Detailed Description
The compute_code_target function determines the proper column position where code should be indented. It considers multiple factors including the current indentation level, parentheses nesting, continuation indentation preferences, and line length constraints. The function implements different indentation strategies based on configuration options like lineup_to_parens and lineup_to_parens_always.

The base indentation is calculated from the current indent level multiplied by the indent size. Additional indentation is added for parentheses levels and statement continuations. When aligning to parentheses, the function may adjust the target to avoid exceeding maximum column width while maintaining readability.

## Parameters / Member Variables
This function takes no parameters but uses several global variables:
- : Current brace/block indentation level
- : Number of spaces per indentation level  
- : Current parentheses nesting depth
- : Flag indicating if this is a continuation of a statement
- : Target column for parentheses alignment
- : Extra indentation for statement continuations
- : Maximum allowed column width

## Dependencies
- Functions called/Symbols referenced:
  - [count_spaces](count_spaces.md) (calculates column position after considering tab expansion)
- Called from (representative examples):
  - [dump_line](../d/dump_line.md) (main line output function)
  - [main](../m/main.md) (in indent.c for various formatting decisions)
  - [pr_comment](../p/pr_comment.md) (for comment positioning relative to code)

## Notes and Other Information
- Returns the target column number (1-based) where code should be positioned
- Handles different parentheses alignment strategies based on configuration flags
- Considers line length limits when determining parentheses alignment
- Critical for maintaining consistent code indentation throughout the formatting process
- Part of the pg_bsd_indent tool's core indentation calculation logic