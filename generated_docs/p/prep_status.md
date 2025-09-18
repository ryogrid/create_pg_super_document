# prep_status

## Location
[src/bin/pg_upgrade/util.c:129-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L129-L155)

## Overview
Displays formatted status messages for operations about to begin in pg_upgrade, providing consistent alignment for subsequent status indicators.

## Definition


## Detailed Description
The  function is a core component of pg_upgrade's user interface system, designed to display descriptive messages about operations that are about to commence. It accepts printf-style format strings and arguments to create informative status messages.

Key features include:
- **Consistent formatting**: Messages are padded to MESSAGE_WIDTH characters to ensure that subsequent "ok" and "failed" indicators align nicely in the output
- **Variadic arguments**: Supports printf-style format strings with variable arguments for flexible message composition  
- **Message truncation**: Overlength messages are automatically truncated to prevent layout issues
- **No-newline output**: Uses PG_REPORT_NONL to allow status indicators to appear on the same line

The typical usage pattern involves calling  before an operation, then following up with either  for success or  for failures.

## Parameters / Member Variables
- : Printf-style format string describing the operation about to begin
- : Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  -  (formats the variadic arguments into a string)
  -  (outputs the formatted message with PG_REPORT_NONL)
  -  (maximum string buffer size constant)
  -  (output formatting width constant)
  -  (log level for non-newline output)
- Called from (representative examples):
  -  in src/bin/pg_upgrade/check.c:346
  -  in src/bin/pg_upgrade/check.c:798
  -  in src/bin/pg_upgrade/dump.c:20
  -  in src/bin/pg_upgrade/pg_upgrade.c:192
  -  in src/bin/pg_upgrade/pg_upgrade.c:491

## Notes and Other Information
- Essential component of pg_upgrade's progress reporting system
- Works in conjunction with  and error logging functions to provide consistent user feedback
- The MESSAGE_WIDTH padding ensures professional-looking aligned output across different message lengths
- Used extensively throughout pg_upgrade for virtually every major operation
- Messages should be kept concise to avoid truncation and maintain readability