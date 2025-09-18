# skip

## Location
src/backend/regex/regc_lex.c: 982 - 1009

## Overview
The `skip` function advances the parser position past whitespace characters and comments when processing regular expressions in expanded form mode.

## Definition
```c
static void skip(struct vars *v)
```

## Detailed Description
The `skip` function is a utility function used in regex parsing that handles whitespace and comment skipping for expanded regular expressions (when REG_EXPANDED flag is set). It implements a two-phase approach:

1. **Whitespace skipping**: Advances past all consecutive whitespace characters using the `iscspace` function
2. **Comment handling**: When encountering a '#' character, skips the entire comment line up to (but not including) the newline character

The function continues this process in a loop until no more whitespace or comments are found. It records the use of non-POSIX features when any characters are actually skipped, as expanded regex syntax is a PostgreSQL extension.

## Parameters / Member Variables
- `v`: Pointer to the regex parsing state structure containing the current position pointer (`now`) and compilation flags (`cflags`)

## Dependencies
- Functions called/Symbols referenced:
  - REG_EXPANDED (flag constant for expanded regex mode)
  - iscspace (character classification function for whitespace)
  - ATEOS (macro for end-of-string detection)
  - CHR (character constant macro)
  - NEXT1 (lookahead macro)
  - NOTE (macro for recording regex features)
  - REG_UNONPOSIX (flag for non-POSIX feature usage)
- Called from (representative examples):
  - next (main tokenizer function)
  - brenext (BRE tokenizer function)
  - Various heap, btree, and utility functions throughout PostgreSQL

## Notes and Other Information
- Part of PostgreSQL's regex engine implementation in src/backend/regex/regc_lex.c:982-1009
- Only operates when REG_EXPANDED flag is set in the regex compilation flags
- Leaves newline characters to be processed by the whitespace loop in the next iteration
- Widely used throughout PostgreSQL codebase beyond just regex parsing, suggesting it may be a general utility function
- Records usage of expanded syntax for compatibility tracking and potential warnings