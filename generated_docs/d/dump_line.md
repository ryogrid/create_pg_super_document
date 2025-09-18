# dump_line

## Location
[src/tools/pg_bsd_indent/io.c:61-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L61-L222)

## Overview
Outputs a formatted line of source code including label, code, and comment sections with proper indentation and spacing.

## Definition


## Detailed Description
The dump_line function is the core output routine for pg_bsd_indent that handles the actual printing of formatted source code lines. It processes three main sections: labels (like goto labels or preprocessor directives), code statements, and comments. The function applies proper indentation, handles blank line insertion/suppression, manages comment positioning, and maintains consistent formatting according to the configured style options.

The function operates on global buffers (s_lab/e_lab for labels, s_code/e_code for code, s_com/e_com for comments) and uses the parser state (ps) to determine proper formatting. It handles special cases like preprocessor directives (#else, #endif), box comments, and ensures proper spacing between different code elements.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables and buffers:
- Uses label buffer (s_lab to e_lab) for goto labels and preprocessor directives
- Uses code buffer (s_code to e_code) for actual C code statements  
- Uses comment buffer (s_com to e_com) for comments
- Accesses parser state (ps) for indentation levels and formatting flags
- Modifies output stream and line counting variables

## Dependencies
- Functions called/Symbols referenced:
  - [compute_label_target](../c/compute_label_target.md) (calculates target column for labels)
  - [compute_code_target](../c/compute_code_target.md) (calculates target column for code)
  - [pad_output](../p/pad_output.md) (outputs spaces to reach target column)
  - [count_spaces](../c/count_spaces.md) (counts column position after text)
- Called from (representative examples):
  - [main](../m/main.md) (primary formatting loop in indent.c)
  - [fill_buffer](../f/fill_buffer.md) (when buffer needs flushing)
  - [pr_comment](../p/pr_comment.md) (when comment processing triggers line output)

## Notes and Other Information
- Handles blank line suppression and insertion based on formatting rules
- Special processing for preprocessor directives (#else, #endif) with conditional comment formatting
- Manages parentheses indentation tracking for multi-line expressions
- Resets all buffers and parser state after outputting each line
- Critical for maintaining proper C code formatting and indentation consistency
- Part of the pg_bsd_indent tool which is PostgreSQL's version of the BSD indent utility