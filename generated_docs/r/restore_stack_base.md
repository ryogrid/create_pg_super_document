# restore_stack_base

## Location
src/backend/tcop/postgres.c: 3541 - 3557

## Overview
restore_stack_base restores a previously saved stack depth checking reference point, primarily used for multi-threaded environments like PL/Java where different threads have different stack locations.

## Definition


## Detailed Description
restore_stack_base provides a mechanism to restore a previously saved stack base reference point that was obtained from set_stack_base(). This function is essential for handling scenarios where PostgreSQL backend functions are called from different execution contexts that have different stack layouts.

The primary use case is with PL/Java, where:
1. A backend function may be called from a different thread than the main PostgreSQL thread
2. Each thread has its own stack location and layout
3. Before calling the backend function, PL/Java calls set_stack_base() to establish a new reference point appropriate for the current thread's stack
4. After the function call completes, restore_stack_base() is called to restore the original reference point

This ensures that PostgreSQL's stack depth checking mechanism works correctly regardless of which thread context the code is executing in.

## Parameters / Member Variables
- : The previously saved stack base reference point of type pg_stack_base_t to restore

## Dependencies
- Functions called/Symbols referenced:
  - pg_stack_base_t (type definition)
  - stack_base_ptr (global variable)
- Called from (representative examples):
  - PL/Java extension (external)
  - Other multi-threaded extensions that need stack context management

## Notes and Other Information
- This function is the counterpart to set_stack_base() and should be called with the value returned by set_stack_base()
- Currently primarily used by PL/Java but available for other extensions that need similar functionality
- Ensures stack overflow protection works correctly in multi-threaded environments
- Simple assignment operation that updates the global stack_base_ptr variable
- Critical for maintaining stack safety when PostgreSQL backend functions are called from different thread contexts