# pr_comment

## Location
src/tools/pg_bsd_indent/pr_comment.c: 79 - 354

## Overview
This function handles the scanning and printing of comments in the pg_bsd_indent tool, managing comment formatting, alignment, and line breaking according to various style options.

## Definition
```c
void pr_comment(void)
```

## Detailed Description
The `pr_comment` function is a comprehensive comment processing routine that forms part of PostgreSQL's BSD-style indenter tool. It handles various types of comments including boxed comments, block comments, and regular comments. The function implements sophisticated logic for:

1. **Comment Alignment**: Determines where comments should be positioned based on surrounding code context
2. **Line Breaking**: Decides whether comment lines should be broken and filled to fit within column limits
3. **Comment Formatting**: Handles different comment styles (boxed comments with `-` or `*`, block comments with newlines)
4. **Content Preservation**: Maintains original formatting for boxed comments while reformatting regular comments

The function processes comments character by character, handling special cases like form feeds, newlines, and comment terminators (`*/`). It implements intelligent line wrapping that preserves word boundaries and maintains proper indentation.

## Parameters / Member Variables
This function takes no parameters but operates on several global state variables:
- Uses `buf_ptr` to read from the input buffer
- Writes formatted output to the comment buffer (`e_com`, `s_com`)
- Accesses formatting options through global configuration variables
- Modifies parser state through the `ps` (parser state) structure

## Dependencies
- Functions called/Symbols referenced:
  - `compute_code_target`: Calculate target column for code alignment
  - `compute_label_target`: Calculate target column for label alignment  
  - `count_spaces`: Count spaces for column calculations
  - `count_spaces_until`: Count spaces from start position to target
  - `dump_line`: Output the current line buffer
  - `fill_buffer`: Read more input when buffer is exhausted
  - `CHECK_SIZE_COM`: Macro to ensure comment buffer has sufficient space
  - `sc_size`: Size of saved comment buffer
  - `lbrace`: Token type for left brace

- Called from (representative examples):
  - `directives`: Main processing function in indent.c:1187
  - Referenced in `nitems`: Header definition in indent.h:50

## Notes and Other Information
- **Comment Types**: Distinguishes between boxed comments (starting with `-` or `*`), block comments (starting with newline), and regular comments
- **Formatting Control**: Respects various formatting flags like `format_col1_comments`, `format_block_comments`, `star_comment_cont`
- **Line Length Management**: Implements adaptive column limits with `adj_max_col` and handles comment overflow intelligently
- **State Preservation**: Maintains comment delta information for proper indentation reconstruction
- **Buffer Management**: Handles input buffer refilling and output buffer size checking throughout processing
- **Historical Context**: Originally written in November 1976 by D A Willcox, with modifications for UNIX-style comments in December 1976

The function is critical for maintaining consistent comment formatting in PostgreSQL's codebase when using the BSD indent tool.