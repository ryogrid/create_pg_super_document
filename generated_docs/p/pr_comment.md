# pr_comment

## Location
[src/tools/pg_bsd_indent/pr_comment.c:79-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/pr_comment.c#L79-L354)

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
  - [compute_code_target](../c/compute_code_target.md): Calculate target column for code alignment
  - [compute_label_target](../c/compute_label_target.md): Calculate target column for label alignment  
  - [count_spaces](../c/count_spaces.md): Count spaces for column calculations
  - [count_spaces_until](../c/count_spaces_until.md): Count spaces from start position to target
  - [dump_line](../d/dump_line.md): Output the current line buffer
  - [fill_buffer](../f/fill_buffer.md): Read more input when buffer is exhausted
  - `CHECK_SIZE_COM`: Macro to ensure comment buffer has sufficient space
  - `sc_size`: Size of saved comment buffer
  - `lbrace`: Token type for left brace

- Called from (representative examples):
  - [directives](../d/directives.md): Main processing function in indent.c:1187
  - Referenced in `nitems`: Header definition in indent.h:50

## Notes and Other Information
- **Comment Types**: Distinguishes between boxed comments (starting with `-` or `*`), block comments (starting with newline), and regular comments
- **Formatting Control**: Respects various formatting flags like `format_col1_comments`, `format_block_comments`, `star_comment_cont`
- **Line Length Management**: Implements adaptive column limits with `adj_max_col` and handles comment overflow intelligently
- **State Preservation**: Maintains comment delta information for proper indentation reconstruction
- **Buffer Management**: Handles input buffer refilling and output buffer size checking throughout processing
- **Historical Context**: Originally written in November 1976 by D A Willcox, with modifications for UNIX-style comments in December 1976

The function is critical for maintaining consistent comment formatting in PostgreSQL's codebase when using the BSD indent tool.

## Simplified Source

```c
void
pr_comment(void)
{
    int now_col;
    int adj_max_col;
    char *last_bl = NULL;
    int break_delim = comment_delimiter_on_blankline;

    ps.box_com = false;
    ++ps.out_coms;

    // Determine comment alignment and formatting
    if (ps.col_1 && !format_col1_comments) {
        // Column 1 comments should not be touched
        ps.box_com = true;
        break_delim = false;
        ps.com_col = 1;
    } else {
        // Check for boxed comments (starting with - or *)
        if (*buf_ptr == '-' || *buf_ptr == '*' ||
            (*buf_ptr == '\n' && !format_block_comments)) {
            ps.box_com = true;
            break_delim = false;
        }

        // Calculate comment column position
        if ((s_lab == e_lab) && (s_code == e_code)) {
            // Blank line - use indentation level
            ps.com_col = (ps.ind_level - ps.unindent_displace) * ps.ind_size + 1;
            adj_max_col = block_comment_max_col;
        } else {
            // Position relative to code or labels
            int target_col = (s_code != e_code) ?
                count_spaces(compute_code_target(), s_code) : 1;
            ps.com_col = ps.decl_on_line || ps.ind_level == 0 ?
                ps.decl_com_ind : ps.com_ind;
            if (ps.com_col <= target_col)
                ps.com_col = tabsize * (1 + (target_col - 1) / tabsize) + 1;
        }
    }

    // Handle comment indentation calculation
    if (ps.box_com) {
        char *start = buf_ptr >= save_com && buf_ptr < save_com + sc_size ?
            sc_buf : in_buffer;
        ps.n_comment_delta = 1 - count_spaces_until(1, start, buf_ptr - 2);
    } else {
        ps.n_comment_delta = 0;
        while (*buf_ptr == ' ' || *buf_ptr == '\t')
            buf_ptr++;
    }

    // Start comment with /*
    *e_com++ = '/';
    *e_com++ = '*';
    if (*buf_ptr != ' ' && !ps.box_com)
        *e_com++ = ' ';

    // Copy comment content
    while (1) {
        switch (*buf_ptr) {
        case '\n':
            // Handle newlines and line breaking
            last_bl = NULL;
            if (ps.box_com || ps.last_nl) {
                if (s_com == e_com)
                    *e_com++ = ' ';
                dump_line();
                if (!ps.box_com && star_comment_cont)
                    *e_com++ = ' ', *e_com++ = '*', *e_com++ = ' ';
            } else {
                ps.last_nl = 1;
                last_bl = e_com;
                *e_com++ = ' ';
            }
            ++line_no;
            ++buf_ptr;
            break;

        case '*':
            ++buf_ptr;
            if (*buf_ptr == '/') {
                // End of comment
                ++buf_ptr;
                if (e_com[-1] != ' ' && e_com[-1] != '\t' && !ps.box_com)
                    *e_com++ = ' ';
                *e_com++ = '*';
                *e_com++ = '/';
                *e_com = '\0';
                return;
            } else {
                *e_com++ = '*';
            }
            break;

        default:
            // Copy regular characters with line wrapping
            now_col = count_spaces_until(ps.com_col, s_com, e_com);
            do {
                *e_com = *buf_ptr++;
                if (*e_com == ' ' || *e_com == '\t')
                    last_bl = e_com;
                ++e_com;
                ++now_col;
            } while (*buf_ptr != '*' && *buf_ptr != '\n' &&
                    (now_col <= adj_max_col || !last_bl));

            // Handle line overflow
            if (now_col > adj_max_col && !ps.box_com && last_bl) {
                *e_com = '\0';
                e_com = last_bl;
                dump_line();
                if (!ps.box_com && star_comment_cont)
                    *e_com++ = ' ', *e_com++ = '*', *e_com++ = ' ';
                // Skip whitespace and continue
                for (char *t_ptr = last_bl + 1; *t_ptr == ' ' || *t_ptr == '\t'; t_ptr++);
                while (*t_ptr != '\0') {
                    if (*t_ptr == ' ' || *t_ptr == '\t')
                        last_bl = e_com;
                    *e_com++ = *t_ptr++;
                }
            }
            break;
        }
    }
}
```