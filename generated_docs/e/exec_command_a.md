# exec_command_a

## Location
[src/bin/psql/command.c:466-484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L466-L484)

## Overview
exec_command_a implements the \a backslash command that toggles between aligned and unaligned field formatting in PostgreSQL psql output.

## Definition


## Detailed Description
exec_command_a provides a simple toggle mechanism for psql's field alignment formatting. When the current format is aligned (PRINT_ALIGNED), it switches to unaligned format; when the current format is anything other than aligned, it switches to aligned format. The function only performs the actual format change when active_branch is true (i.e., when not within a false \if conditional block).

The implementation uses the do_pset() function to change the "format" setting, which is the same mechanism used by the \pset command. According to the comment, this command "makes little sense" but is kept for backward compatibility.

## Parameters / Member Variables
- `scan_state`: Lexer working state (not used in this function, but required for interface consistency)
- `active_branch`: Boolean indicating whether the command should actually execute (false when inside a false \if block)

## Dependencies
- Functions called/Symbols referenced:
  - do_pset
  - PRINT_ALIGNED (constant)
  - pset global variable (print options)
- Called from (representative examples):
  - [exec_command](exec_command.md) (src/bin/psql/command.c:329)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- The command takes no arguments - it's a pure toggle operation
- Only executes when active_branch is true, allowing proper conditional command handling
- Accesses global pset.popt.topt.format to determine current formatting state
- Uses the same underlying mechanism (do_pset) as the \pset format command
- Considered somewhat legacy functionality but maintained for compatibility