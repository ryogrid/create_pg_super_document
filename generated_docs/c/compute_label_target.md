# compute_label_target

## Location
[src/tools/pg_bsd_indent/io.c:252-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L252-L274)

## Overview
Calculates the target column position for labels including goto labels, case labels, and preprocessor directives.

## Definition

```c
int
compute_label_target(void)
```
## Detailed Description
The compute_label_target function determines the proper column position for various types of labels in C code. It handles three distinct cases: case/default labels within switch statements, preprocessor directives (which start with #), and regular goto labels. The function uses different indentation rules for each type to ensure proper code formatting and readability.

For case labels, it uses the case_ind setting multiplied by the indent size. Preprocessor directives are always positioned at column 1 regardless of nesting level. Regular goto labels are positioned based on the current indentation level minus the label_offset setting, ensuring they stand out from regular code while maintaining proper alignment.

## Parameters / Member Variables
This function takes no parameters but uses several global variables:
- : Boolean flag indicating if currently processing a case/default label
- : Number of spaces per indentation level
- : Current block/brace indentation level
- : Pointer to start of label buffer (checked for '#' prefix)
- : Indentation multiplier for case labels
- : How much to outdent regular goto labels from current level

## Dependencies
- Functions called/Symbols referenced:
  - label_offset (global variable for label positioning)
- Called from (representative examples):
  - [dump_line](../d/dump_line.md) (main line output function when processing labels)
  - [pr_comment](../p/pr_comment.md) (for comment positioning relative to labels)

## Notes and Other Information
- Returns the target column number (1-based) where labels should be positioned
- Preprocessor directives (#define, #include, etc.) are always placed at column 1
- Case/default labels use special indentation rules separate from regular code blocks
- Regular goto labels are typically outdented from the current block level for visibility
- Critical for maintaining consistent label formatting in the pg_bsd_indent tool

## Simplified Source

```c
int compute_label_target(void) {
    // Case labels: use case indentation
    if (ps.pcase) {
        return (int) (case_ind * ps.ind_size) + 1;
    }

    // Preprocessor directives: always at column 1
    if (*s_lab == '#') {
        return 1;
    }

    // Regular goto labels: outdented from current level
    return ps.ind_size * (ps.ind_level - label_offset) + 1;
}
```