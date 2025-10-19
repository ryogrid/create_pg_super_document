# printPsetInfo

## Location
[src/bin/psql/command.c:4886-5085](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L4886-L5085)

## Overview
A comprehensive display function in psql that prints the current state of formatting parameters, providing users with detailed information about their output settings.

## Definition
```c
static bool printPsetInfo(const char *param, printQueryOpt *popt)
```

## Detailed Description
The `printPsetInfo` function serves as the primary interface for displaying psql's formatting parameter states to users. It takes a parameter name and the current print options structure, then outputs a human-readable description of the specified setting's current value. This function is central to psql's \\pset command when used without values (to show current settings). It handles a comprehensive range of formatting options including border styles, field separators, output formats, pager settings, Unicode line styles, and various display toggles. The function uses internationalized messages and provides detailed feedback about each formatting parameter's current state.

## Parameters / Member Variables
- `param`: String specifying which formatting parameter to display (e.g., "border", "format", "fieldsep")
- `popt`: Pointer to printQueryOpt structure containing the current formatting settings to be displayed

## Dependencies
- Functions called/Symbols referenced:
  - [_align2string](../a/_align2string.md) (converts format enum to string representation)
  - [_unicode_linestyle2string](../u/_unicode_linestyle2string.md) (converts Unicode line style enum to string)
  - [get_line_style](../g/get_line_style.md) (retrieves line style information)
  - ngettext (internationalization function for plural forms)
  - Various enum constants (PRINT_XHEADER_*, etc.)
- Called from (representative examples):
  - fmt (formatting command handler in command.c)

## Notes and Other Information
- The function is declared as static, limiting its scope to the command.c compilation unit
- Returns true on successful parameter display, false if the parameter name is unknown
- Supports all major psql formatting parameters: border, columns, expanded display, field separators, footer, format, line style, null display, numeric locale, pager settings, record separators, table attributes, title, tuples-only mode, and Unicode line styles
- Uses internationalized messages through gettext macros (_() and ngettext()) for localization support
- Handles special cases like zero-byte separators and different expanded header width types
- Provides detailed, user-friendly descriptions of complex formatting states
- Part of psql's comprehensive formatting system that allows users to inspect their current output settings
- Error handling includes logging unknown parameter names with pg_log_error

## Simplified Source

```c
static bool printPsetInfo(const char *param, printQueryOpt *popt) {
    // Display current formatting parameter values based on param name

    if (strcmp(param, "border") == 0) {
        printf(_("Border style is %d.\n"), popt->topt.border);
    }
    else if (strcmp(param, "columns") == 0) {
        if (!popt->topt.columns)
            printf(_("Target width is unset.\n"));
        else
            printf(_("Target width is %d.\n"), popt->topt.columns);
    }
    else if (strcmp(param, "expanded") == 0) {
        // Handle expanded display modes (off/on/auto)
        if (popt->topt.expanded == 1)
            printf(_("Expanded display is on.\n"));
        else if (popt->topt.expanded == 2)
            printf(_("Expanded display is used automatically.\n"));
        else
            printf(_("Expanded display is off.\n"));
    }
    else if (strcmp(param, "format") == 0) {
        printf(_("Output format is %s.\n"), _align2string(popt->topt.format));
    }
    else if (strcmp(param, "null") == 0) {
        printf(_("Null display is \"%s\".\n"),
               popt->nullPrint ? popt->nullPrint : "");
    }
    else if (strcmp(param, "pager") == 0) {
        // Handle pager usage modes
        if (popt->topt.pager == 1)
            printf(_("Pager is used for long output.\n"));
        else if (popt->topt.pager == 2)
            printf(_("Pager is always used.\n"));
        else
            printf(_("Pager usage is off.\n"));
    }
    // ... additional parameter checks for other formatting options ...
    else {
        pg_log_error("\\pset: unknown option: %s", param);
        return false;
    }

    return true;
}
```