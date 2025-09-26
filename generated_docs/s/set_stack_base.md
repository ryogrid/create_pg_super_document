# set_stack_base

## Location
[src/backend/tcop/postgres.c:3508-3540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3508-L3540)

## Overview
set_stack_base establishes a reference point for PostgreSQL's stack depth checking mechanism, allowing the system to monitor and prevent stack overflow conditions.

## Definition

```c
pg_stack_base_t
set_stack_base(void)
```
## Detailed Description
set_stack_base initializes the stack depth monitoring system by setting a reference point (stack_base_ptr) that subsequent stack depth checks can use to measure how deep the call stack has grown. The function uses platform-specific methods to obtain the current stack frame address:

- On systems with __builtin_frame_address() support (recent GCC), it uses this built-in function to avoid compiler warnings about storing local variable addresses
- On other systems, it takes the address of a local variable as the reference point

The function is typically called early in process initialization to establish the baseline for stack depth measurements. This baseline is crucial for PostgreSQL's stack overflow protection mechanism, which prevents runaway recursion that could crash the server.

## Parameters / Member Variables
This function takes no parameters and returns the previous stack base reference point.

## Dependencies
- Functions called/Symbols referenced:
  - pg_stack_base_t (type definition)
  - stack_base_ptr (global variable)
  - __builtin_frame_address() (when available)
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [InitPostmasterChild](../I/InitPostmasterChild.md)

## Notes and Other Information
- The function returns the old reference point, allowing for nested or temporary stack base changes that can be restored later
- Uses conditional compilation to choose between __builtin_frame_address() and local variable address methods
- The HAVE__BUILTIN_FRAME_ADDRESS macro determines which implementation path is used
- This is part of PostgreSQL's defensive programming approach to prevent stack overflow crashes
- The stack base pointer is stored in the global variable stack_base_ptr for use by other stack depth checking functions