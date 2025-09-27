# interactive_getc

## Location
[src/backend/tcop/postgres.c:336-363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L336-L363)

## Overview
A signal-aware character input function that safely reads one character from stdin while handling PostgreSQL interrupts and signals.

## Definition

```c
static int
interactive_getc(void)
```
## Detailed Description
The `interactive_getc` function is a specialized character input function designed for PostgreSQL's interactive backend mode. Unlike the standard `getc()` function, it incorporates PostgreSQL's signal handling mechanisms to ensure that the backend can properly respond to signals like SIGTERM and SIGQUIT even while waiting for user input.

The function performs interrupt checking before reading input and processes client read interrupts after obtaining the character. This ensures that the interactive backend remains responsive to system signals and can be terminated gracefully. The function is specifically designed for standalone backend processes where traditional client-server interrupt handling may not be available.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS() (PostgreSQL interrupt handling macro)
  - getc() (standard C library function)
  - [ProcessClientReadInterrupt](../P/ProcessClientReadInterrupt.md)() (PostgreSQL interrupt processing)
  - stdin (standard input stream)

- Called from (representative examples):
  - [InteractiveBackend](../I/InteractiveBackend.md) (src/backend/tcop/postgres.c:263)

## Notes and Other Information
- This function is static, meaning it's only accessible within the postgres.c compilation unit
- The function is specifically designed for standalone backend processes, not typical client-server scenarios
- It does not process catchup interrupts or notifications during reading, as these are not relevant for standalone backends
- The interrupt handling is simplified compared to full client-server scenarios due to the standalone nature
- There is special handling in the die() function to directly process interrupts at this stage for proper SIGTERM handling
- The function maintains the same return semantics as standard getc(), including EOF on end-of-file or error conditions
- This function is essential for making PostgreSQL's interactive mode responsive to system signals while maintaining the simplicity of character-by-character input processing

## Simplified Source

```c
// Simplified version of interactive_getc
static int interactive_getc(void) {
    // Check for pending interrupts before reading input
    // This allows the process to respond to SIGTERM/SIGQUIT signals
    CHECK_FOR_INTERRUPTS();

    // Read one character from standard input
    int character = getc(stdin);

    // Process any client read interrupts that may have occurred
    // Pass false to indicate we're not in a blocked state
    ProcessClientReadInterrupt(false);

    return character;
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Used more descriptive variable name (character vs c)
- Clarified the purpose of interrupt checking before and after input
- Preserved essential signal handling for standalone backend processes
- Maintained compatibility with standard getc() return semantics
- Emphasized the signal responsiveness aspect of the function