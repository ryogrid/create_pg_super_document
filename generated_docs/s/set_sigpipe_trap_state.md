# set_sigpipe_trap_state

## Location
[src/fe_utils/print.c:3075-3088](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3075-L3088)

## Overview
Sets the trap state for SIGPIPE signal handling to determine whether SIGPIPE should be ignored or not.

## Definition

```c
struct winsize screen_size;
```
## Detailed Description
This function configures the global state for SIGPIPE signal handling by setting the  variable. It is used in conjunction with  to manage SIGPIPE signal handling in PostgreSQL frontend utilities. The function allows the application to specify whether SIGPIPE signals should be ignored during certain operations, which is particularly important when writing to pipes or sockets that might be closed by the receiving end.

## Parameters / Member Variables
- : Boolean flag indicating whether SIGPIPE should be ignored (true) or handled normally (false)

## Dependencies
- Functions called/Symbols referenced:
  - (none - only sets a global variable)
- Called from (representative examples):
  - [setQFout](setQFout.md) (src/bin/psql/common.c:154)

## Notes and Other Information
- This function is part of the signal handling infrastructure in PostgreSQL frontend utilities
- It works in conjunction with signal trapping mechanisms to control SIGPIPE behavior
- The function modifies the global variable  to store the desired state
- Used primarily in psql and other frontend tools where pipe handling is critical

## Simplified Source

```c
void set_sigpipe_trap_state(bool should_ignore_sigpipe) {
    // Set global flag to control SIGPIPE signal handling
    always_ignore_sigpipe = should_ignore_sigpipe;
}
```

This simplified version preserves the core functionality:
- Sets the global state variable for SIGPIPE handling
- Takes a boolean parameter to determine ignore behavior
- Simple assignment operation with descriptive parameter name
- Essential for controlling pipe signal handling in frontend utilities