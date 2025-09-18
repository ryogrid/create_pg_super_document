# pg_trigger_depth

## Location
[src/backend/commands/trigger.c:6680-6683](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L6680-L6683)

## Overview
Returns the current nesting level of PostgreSQL triggers, allowing stored procedures to determine how deeply nested they are within trigger execution.

## Definition


## Detailed Description
The  function is a SQL-callable system function that returns the current depth of trigger execution nesting. This function provides visibility into how many levels deep the system is currently executing triggers, which is useful for debugging complex trigger scenarios and preventing excessive recursion.

The function returns:
-  when called from outside any trigger context
-  when called directly from within a trigger
-  or higher when called from triggers that were themselves called by other triggers

The depth tracking is implemented through the global variable , which is incremented when entering trigger execution and decremented when exiting, ensuring accurate nesting level tracking even in the presence of exceptions.

## Parameters / Member Variables
This function takes no parameters (uses  macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  -  (global static variable)
  -  (macro)
- Called from (representative examples):
  - User-defined trigger functions
  - SQL queries for debugging trigger behavior
  - Test functions in regression tests

## Notes and Other Information
- The function is marked as stable () and parallel restricted () in the system catalog
-  is a static variable in  that tracks the current trigger execution depth
- The depth counter is managed using PostgreSQL's exception handling mechanism () to ensure proper cleanup even when triggers raise exceptions
- This function is particularly useful for preventing infinite trigger recursion by allowing triggers to check their execution depth
- The function is defined in the system catalog () and is available as a built-in SQL function
- Comprehensive test cases exist in the regression test suite () demonstrating various trigger depth scenarios