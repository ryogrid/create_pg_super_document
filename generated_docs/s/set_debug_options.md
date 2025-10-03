# set_debug_options

## Location
[src/backend/tcop/postgres.c:3766-3794](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3766-L3794)

## Overview
A utility function that applies debug-level configurations based on the "-d N" command line option, progressively enabling more verbose logging and debugging output as the debug level increases.

## Definition

```c
void
set_debug_options(int debug_flag, GucContext context, GucSource source)
```
## Detailed Description
This function implements PostgreSQL's debug level functionality triggered by the "-d N" command line option. It sets various logging and debugging configuration options based on the specified debug level, with higher levels enabling increasingly detailed output. The function provides a convenient way to configure multiple related debugging parameters with a single debug level value, making it easier for developers and administrators to control the verbosity of PostgreSQL's diagnostic output.

## Parameters / Member Variables
- `debug_flag`: The debug level (integer value). Higher values enable more verbose debugging output. Level 0 disables debug output.
- `context`: The GUC context indicating when/how this configuration change is being applied (e.g., PGC_POSTMASTER, PGC_SIGHUP, etc.)
- `source`: The source of the configuration change (e.g., command line, configuration file, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - sprintf: Formats the debug level into a string for log_min_messages
  - [SetConfigOption](../S/SetConfigOption.md): Sets individual GUC parameters programmatically
  - PGC_POSTMASTER: Context constant for postmaster-level configuration changes
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md): Sets debug options during postmaster startup
  - [process_postgres_switches](../p/process_postgres_switches.md): Processes command-line switches including -d option

## Notes and Other Information
- Debug levels have cumulative effects: higher levels include all lower level settings
- Level 1+: Enables connection/disconnection logging (only in postmaster context)
- Level 2+: Enables logging of all SQL statements
- Level 3+: Enables parse tree debugging output
- Level 4+: Enables execution plan debugging output  
- Level 5+: Enables rewritten query debugging output
- The function differs from simply setting log_min_messages as it enables additional specialized debugging options
- Connection/disconnection logging is only enabled in PGC_POSTMASTER context for security and performance reasons

## Simplified Source

```c
// Simplified version of set_debug_options
void set_debug_options(int debug_flag, GucContext context, GucSource source) {
    // Step 1: Set base logging level based on debug flag
    if (debug_flag > 0) {
        char debugstr[64];
        sprintf(debugstr, "debug%d", debug_flag);
        SetConfigOption("log_min_messages", debugstr, context, source);
    } else {
        SetConfigOption("log_min_messages", "notice", context, source);
    }

    // Step 2: Enable connection logging for debug level 1+ (postmaster context only)
    if (debug_flag >= 1 && context == PGC_POSTMASTER) {
        SetConfigOption("log_connections", "true", context, source);
        SetConfigOption("log_disconnections", "true", context, source);
    }

    // Step 3: Progressively enable more detailed debugging output
    if (debug_flag >= 2)
        SetConfigOption("log_statement", "all", context, source);

    if (debug_flag >= 3)
        SetConfigOption("debug_print_parse", "true", context, source);

    if (debug_flag >= 4)
        SetConfigOption("debug_print_plan", "true", context, source);

    if (debug_flag >= 5)
        SetConfigOption("debug_print_rewritten", "true", context, source);
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Grouped related functionality with clear step labels
- Maintained the original logic flow and conditional structure
- Enhanced readability with better code organization
- Preserved all essential functionality and parameter handling