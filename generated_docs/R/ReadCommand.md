# ReadCommand

## Location
src/backend/tcop/postgres.c: 492 - 512

## Overview
A dispatcher function that routes command reading to either socket-based or interactive input depending on the output destination configuration.

## Definition


## Detailed Description
The `ReadCommand` function serves as a command input dispatcher in PostgreSQL's backend process. It abstracts the source of command input by examining the global `whereToSendOutput` variable to determine whether the backend is connected to a remote client (socket-based communication) or running in interactive mode (stdin/stdout).

When `whereToSendOutput` is set to `DestRemote`, indicating a client-server connection, the function delegates to `SocketBackend()` to handle PostgreSQL's wire protocol communication. Otherwise, it uses `InteractiveBackend()` for direct terminal interaction, which is typically used for debugging or standalone backend execution.

This abstraction allows the rest of the PostgreSQL backend to process commands uniformly regardless of whether they came from a network client or interactive terminal input.

## Parameters / Member Variables
- `inBuf`: A StringInfo buffer where the command or message data will be stored after being read from the appropriate input source.

## Dependencies
- Functions called/Symbols referenced:
  - whereToSendOutput (global variable indicating output destination)
  - DestRemote (constant for remote client destination)
  - SocketBackend() (handles client-server protocol communication)
  - InteractiveBackend() (handles interactive terminal input)

- Called from (representative examples):
  - PostgresMain (src/backend/tcop/postgres.c:4699)

## Notes and Other Information
- This function is static, meaning it's only accessible within the postgres.c compilation unit
- The function provides a clean abstraction layer that hides the complexity of different input sources from the main message processing loop
- The return value follows the same convention as both backend functions: message type code for valid input, or EOF for end-of-file/disconnect conditions
- The choice between socket and interactive backends is made at runtime based on how the PostgreSQL backend process was started
- This design pattern allows PostgreSQL to support both production client-server scenarios and development/debugging scenarios with the same core message processing logic
- The function is a simple dispatcher with no complex logic of its own, delegating all actual work to the appropriate specialized backend function