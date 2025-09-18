# psql_cancel_callback

## Location
[src/bin/psql/common.c:297-312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L297-L312)

## Overview
psql_cancel_callback is a static callback function that handles cancellation requests (typically from SIGINT/Ctrl+C) in psql, providing graceful interruption of operations.

## Definition


## Detailed Description
psql_cancel_callback serves as the signal handler callback for interruption requests in psql. When a cancellation signal is received (such as SIGINT from Ctrl+C), this function determines the appropriate response based on the current state of the application. If psql is currently waiting for input and interrupts are enabled, it performs a non-local jump using siglongjmp() to immediately exit the input waiting state. Otherwise, it sets a global cancel flag that can be checked by long-running operations to allow for graceful termination. The function includes platform-specific behavior, with the longjmp mechanism only available on non-Windows platforms.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - siglongjmp (for non-local jumps on non-Windows platforms)
  - sigint_interrupt_enabled (global variable for interrupt state)
  - sigint_interrupt_jmp (global jump buffer)
  - cancel_pressed (global cancellation flag)
- Called from (representative examples):
  - [psql_setup_cancel_handler](psql_setup_cancel_handler.md) (registers this as the cancel callback)

## Notes and Other Information
- This is a static function, only accessible within the common.c compilation unit
- Uses conditional compilation for Windows vs. non-Windows platforms
- The longjmp mechanism provides immediate interruption when waiting for input
- The cancel_pressed flag provides a polling-based cancellation mechanism for long operations
- Part of psql's signal handling and graceful shutdown infrastructure
- Works in conjunction with psql_setup_cancel_handler for complete interrupt handling