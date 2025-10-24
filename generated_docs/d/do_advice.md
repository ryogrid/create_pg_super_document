# do_advice

## Location
[src/bin/pg_ctl/pg_ctl.c:1953-1960](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L1953-L1960)

## Overview
Displays a helpful message directing users to use the --help option for more information about pg_ctl usage.

## Definition

```c
static void
do_advice(void)
```
## Detailed Description
This is a simple utility function that outputs a standardized help message to stderr. It provides users with guidance on how to get more detailed information about pg_ctl command-line options and usage. The function uses the global progname variable to display the correct program name in the help message, making it consistent with the actual invocation method used by the user.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [write_stderr](../w/write_stderr.md)
  - progname (global variable)
- Called from (representative examples):
  - [set_mode](../s/set_mode.md)
  - [set_sig](../s/set_sig.md)
  - [set_starttype](../s/set_starttype.md)
  - [main](../m/main.md) (multiple locations)

## Notes and Other Information
- The function is marked as static, limiting its scope to the pg_ctl.c file
- Uses write_stderr to ensure the message goes to stderr, which is the conventional output stream for help messages
- The message is internationalized using the _() macro for translation support
- This function is typically called when pg_ctl encounters invalid command-line arguments or usage errors
- Provides a consistent user experience by always directing users to the same help mechanism
- The function serves as a centralized way to provide usage guidance across different error conditions in pg_ctl

## Simplified Source

```c
static void
do_advice(void)
{
    // Display standard help message directing user to --help option
    write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
}
```