# pset_value_string

## Location
[src/bin/psql/command.c:5193-5271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5193-L5271)

## Overview
Returns a formatted string representation of a specific psql print setting parameter value, suitable for display or re-input into the \pset command.

## Definition

```c
static char *
pset_value_string(const char *param, printQueryOpt *popt)
```
## Detailed Description
The pset_value_string function serves as a comprehensive formatter for all psql print settings parameters. It takes a parameter name and a printQueryOpt structure, then returns an appropriately formatted string representation of that parameter's current value. The function handles various data types including integers, booleans, strings, and enums, ensuring that each value is formatted in a way that can be fed back into the \pset command to recreate the same setting.

The function uses a large if-else chain to handle each supported parameter, with special formatting rules for each type. String parameters are quoted and escaped using pset_quoted_string, boolean parameters use pset_bool_string for "on"/"off" representation, and numeric parameters use psprintf for integer formatting. Special cases include the "expanded" parameter which can be "auto", "on", or "off", and the "xheader_width" parameter which supports multiple modes.

## Parameters / Member Variables
- `*param`: Name of the parameter to format (must not be NULL)
- `*popt`: Pointer to the printQueryOpt structure containing current settings
## Dependencies
- Functions called/Symbols referenced:
  - [pset_quoted_string](pset_quoted_string.md) (for string parameter formatting)
  - [pset_bool_string](pset_bool_string.md) (for boolean parameter formatting)
  - [psprintf](psprintf.md) (for integer formatting)
  - [pstrdup](pstrdup.md) (for string duplication)
  - [_align2string](../a/_align2string.md) (for format enum conversion)
  - [get_line_style](../g/get_line_style.md) (for line style information)
  - [_unicode_linestyle2string](../u/_unicode_linestyle2string.md) (for Unicode line style conversion)
  - snprintf (for custom formatting)
- Called from (representative examples):
  - [exec_command_pset](../e/exec_command_pset.md)

## Notes and Other Information
- Returns a newly allocated string that must be freed by the caller
- Handles NULL parameter values appropriately, often returning empty strings
- The "expanded" parameter has special logic for auto mode (value 2)
- [String](../S/String.md) parameters distinguish between unset (NULL) and empty string cases
- The "xheader_width" parameter supports multiple width types (full, column, page, exact)
- Returns "ERROR" for unrecognized parameter names
- All boolean values use PostgreSQL's "on"/"off" convention
- [String](../S/String.md) values are properly quoted and escaped to handle special characters
- Static function scope limits usage to within command.c
- Essential for implementing the \pset command's parameter display functionality

## Simplified Source

```c
static char *pset_value_string(const char *param, printQueryOpt *popt) {
    Assert(param != NULL);

    // Numeric parameters
    if (strcmp(param, "border") == 0)
        return psprintf("%d", popt->topt.border);
    else if (strcmp(param, "columns") == 0)
        return psprintf("%d", popt->topt.columns);
    else if (strcmp(param, "pager") == 0)
        return psprintf("%d", popt->topt.pager);
    else if (strcmp(param, "pager_min_lines") == 0)
        return psprintf("%d", popt->topt.pager_min_lines);

    // String parameters (quoted)
    else if (strcmp(param, "csv_fieldsep") == 0)
        return pset_quoted_string(popt->topt.csvFieldSep);
    else if (strcmp(param, "fieldsep") == 0)
        return pset_quoted_string(popt->topt.fieldSep.separator
                                 ? popt->topt.fieldSep.separator : "");
    else if (strcmp(param, "recordsep") == 0)
        return pset_quoted_string(popt->topt.recordSep.separator
                                 ? popt->topt.recordSep.separator : "");
    else if (strcmp(param, "null") == 0)
        return pset_quoted_string(popt->nullPrint ? popt->nullPrint : "");
    else if (strcmp(param, "tableattr") == 0)
        return popt->topt.tableAttr ? pset_quoted_string(popt->topt.tableAttr)
                                    : pstrdup("");
    else if (strcmp(param, "title") == 0)
        return popt->title ? pset_quoted_string(popt->title) : pstrdup("");

    // Boolean parameters
    else if (strcmp(param, "fieldsep_zero") == 0)
        return pstrdup(pset_bool_string(popt->topt.fieldSep.separator_zero));
    else if (strcmp(param, "footer") == 0)
        return pstrdup(pset_bool_string(popt->topt.default_footer));
    else if (strcmp(param, "numericlocale") == 0)
        return pstrdup(pset_bool_string(popt->topt.numericLocale));
    else if (strcmp(param, "recordsep_zero") == 0)
        return pstrdup(pset_bool_string(popt->topt.recordSep.separator_zero));
    else if (strcmp(param, "tuples_only") == 0)
        return pstrdup(pset_bool_string(popt->topt.tuples_only));

    // Special case parameters
    else if (strcmp(param, "expanded") == 0)
        return pstrdup(popt->topt.expanded == 2 ? "auto"
                      : pset_bool_string(popt->topt.expanded));
    else if (strcmp(param, "format") == 0)
        return pstrdup(_align2string(popt->topt.format));
    else if (strcmp(param, "linestyle") == 0)
        return pstrdup(get_line_style(&popt->topt)->name);

    // Unicode line style parameters
    else if (strcmp(param, "unicode_border_linestyle") == 0)
        return pstrdup(_unicode_linestyle2string(popt->topt.unicode_border_linestyle));
    else if (strcmp(param, "unicode_column_linestyle") == 0)
        return pstrdup(_unicode_linestyle2string(popt->topt.unicode_column_linestyle));
    else if (strcmp(param, "unicode_header_linestyle") == 0)
        return pstrdup(_unicode_linestyle2string(popt->topt.unicode_header_linestyle));

    // Complex xheader_width parameter
    else if (strcmp(param, "xheader_width") == 0) {
        if (popt->topt.expanded_header_width_type == PRINT_XHEADER_FULL)
            return pstrdup("full");
        else if (popt->topt.expanded_header_width_type == PRINT_XHEADER_COLUMN)
            return pstrdup("column");
        else if (popt->topt.expanded_header_width_type == PRINT_XHEADER_PAGE)
            return pstrdup("page");
        else {
            // PRINT_XHEADER_EXACT_WIDTH
            char wbuff[32];
            snprintf(wbuff, sizeof(wbuff), "%d",
                    popt->topt.expanded_header_exact_width);
            return pstrdup(wbuff);
        }
    }

    // Unknown parameter
    else
        return pstrdup("ERROR");
}
```