# end_progress_output

## Location
[src/bin/pg_upgrade/util.c:43-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L43-L62)

## Overview
Finalizes progress output formatting by clearing or indenting the current progress line in pg_upgrade utility.

## Definition

```c
void
end_progress_output(void)
```
## Detailed Description
The  function is responsible for properly terminating progress output in the pg_upgrade utility. It handles two different output scenarios:

1. **TTY output**: When output is directed to a terminal (tty), it erases the current progress line by printing a carriage return and then filling the line with spaces up to MESSAGE_WIDTH characters.
2. **Verbose non-TTY output**: When running in verbose mode but not outputting to a terminal, it simply indents the output with spaces to align with subsequent report_status() output.

The function ensures that progress indicators are cleanly terminated and that subsequent status messages are properly aligned, maintaining a consistent and readable output format throughout the pg_upgrade process.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  -  (with PG_REPORT_NONL flag)
  -  (constant defining output width)
  -  (log level constant)
  -  (global logging option)
  -  (global logging option)
- Called from (representative examples):
  -  in src/bin/pg_upgrade/dump.c:69
  -  in src/bin/pg_upgrade/pg_upgrade.c:642
  -  in src/bin/pg_upgrade/pg_upgrade.c:975
  -  in src/bin/pg_upgrade/relfilenumber.c:77

## Notes and Other Information
- This function is part of the pg_upgrade utility's progress reporting system
- Works in conjunction with other progress reporting functions to provide user feedback during upgrade operations
- The MESSAGE_WIDTH constant ensures consistent formatting across different progress messages
- The function handles both interactive (TTY) and batch/scripted execution modes appropriately

## Simplified Source

```c
void end_progress_output(void) {
    // For TTY output: erase progress line and align for next status
    if (log_opts.isatty) {
        printf("\r");  // Return to beginning of line
        pg_log(PG_REPORT_NONL, "%-*s", MESSAGE_WIDTH, "");  // Clear with spaces
    }
    // For verbose non-TTY: just indent for alignment
    else if (log_opts.verbose) {
        pg_log(PG_REPORT_NONL, "%-*s", MESSAGE_WIDTH, "");
    }
}
```