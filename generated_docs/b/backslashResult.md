# backslashResult

## Location
src/bin/psql/command.h: 24 - 49

## Overview
The backslashResult enum defines the possible return codes from psql backslash command processing, indicating the action that should be taken after a backslash command is executed.

## Definition


## Detailed Description
The backslashResult enumeration serves as a control flow mechanism for psql's command processing system. When a backslash command (such as \d, \q, \c, etc.) is processed by the HandleSlashCmds function, it returns one of these enum values to indicate what action the main command loop should take next. This design allows for clean separation between command parsing/execution and the overall program flow control.

The enum values guide the main loop's behavior: whether to send a completed query to the database server, continue building a multi-line query, terminate the program, handle query buffer modifications, or deal with error conditions. This mechanism is central to psql's interactive command processing architecture.

## Parameters / Member Variables
- : Internal state indicating command parsing is not yet complete
- : Signals that a complete query has been assembled and should be sent to the PostgreSQL server
- : Indicates the current line should be skipped and query building should continue (used for multi-line queries)
- : Requests program termination (typically from \q command)
- : Indicates the query buffer has been modified by an editing command (such as \e for external editor)
- : Signals that an error occurred during backslash command execution

## Dependencies
- Functions called/Symbols referenced:
  - HandleSlashCmds (uses this as return type)
  - PsqlScanState
  - ConditionalStack
  - PQExpBuffer
- Called from (representative examples):
  - MainLoop (in src/bin/psql/mainloop.c:48)
  - Various exec_command_* functions throughout src/bin/psql/command.c

## Notes and Other Information
This enum is fundamental to psql's command processing architecture and is used extensively throughout the psql codebase. The return values from HandleSlashCmds function directly control the main program loop behavior. The PSQL_CMD_UNKNOWN value is primarily for internal use during command parsing phases. Error handling in psql relies heavily on the PSQL_CMD_ERROR return value to properly manage failed backslash commands while maintaining program stability.