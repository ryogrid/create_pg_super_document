# indent_declaration

## Location
[src/tools/pg_bsd_indent/indent.c:1243-1275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/indent.c#L1243-L1275)

## Overview
The indent_declaration function handles proper indentation formatting for variable declarations in the pg_bsd_indent tool.

## Definition

```c
static void
indent_declaration(int cur_dec_ind, int tabs_to_var)
```
## Detailed Description
This function is responsible for correctly positioning and formatting variable declarations within the pg_bsd_indent code formatter. It calculates the proper indentation based on the current indentation level and desired column position, taking into account tab size settings and PostgreSQL-specific tab rules. The function handles both tab-based and space-based indentation, ensuring that declarations are aligned according to the specified formatting rules. It also manages the transition between tabs and spaces when the indentation doesn't align perfectly with tab boundaries.

## Parameters / Member Variables
- `cur_dec_ind`: The target column position for the declaration
- `tabs_to_var`: Boolean flag indicating whether to use tabs for indentation to the variable

## Dependencies
- Functions called/Symbols referenced:
  - [CHECK_SIZE_CODE](../C/CHECK_SIZE_CODE.md) (macro to ensure sufficient buffer space)
- Called from (representative examples):
  - [main](../m/main.md) (multiple locations in pg_bsd_indent processing loop)

## Notes and Other Information
- Works with global variables: ps.ind_level, ps.ind_size, tabsize, postgres_tab_rules
- Modifies global code buffer pointers: e_code, s_code
- Handles PostgreSQL-specific tab rules when postgres_tab_rules is enabled
- Manages ps.want_blank flag to control spacing
- Uses a hybrid approach of tabs and spaces for optimal formatting
- Ensures proper alignment even when indentation levels don't match tab boundaries
- Part of the pg_bsd_indent tool's declaration formatting subsystem

## Simplified Source

```c
static void indent_declaration(int cur_dec_ind, int tabs_to_var)
{
    int pos = e_code - s_code;
    char *startpos = e_code;

    // Adjust for indentation that doesn't align with tab boundaries
    if ((ps.ind_level * ps.ind_size) % tabsize != 0) {
        pos += (ps.ind_level * ps.ind_size) % tabsize;
        cur_dec_ind += (ps.ind_level * ps.ind_size) % tabsize;
    }

    // Use tabs when requested and beneficial
    if (tabs_to_var) {
        int tpos;
        CHECK_SIZE_CODE(cur_dec_ind / tabsize);

        while ((tpos = tabsize * (1 + pos / tabsize)) <= cur_dec_ind) {
            // Apply PostgreSQL tab rules or use standard tabbing
            *e_code++ = (!postgres_tab_rules ||
                        tpos != pos + 1 ||
                        cur_dec_ind >= tpos + tabsize) ? '\t' : ' ';
            pos = tpos;
        }
    }

    // Fill remaining space with spaces to reach target position
    CHECK_SIZE_CODE(cur_dec_ind - pos + 1);
    while (pos < cur_dec_ind) {
        *e_code++ = ' ';
        pos++;
    }

    // Add a space if needed and no indentation was added
    if (e_code == startpos && ps.want_blank) {
        *e_code++ = ' ';
        ps.want_blank = false;
    }
}
```