# injection_points_cleanup

## Location
[src/test/modules/injection_points/injection_points.c:159-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L159-L177)

## Overview
A static cleanup callback function that removes all locally attached injection points when a process is about to exit.

## Definition

```c
static void
injection_points_cleanup(int code, Datum arg)
```
## Detailed Description
This function serves as a  callback that performs cleanup of injection points that are locally attached to the current process. It iterates through the list of locally attached injection points and detaches each one using . This ensures that injection points are properly cleaned up when a process exits, preventing resource leaks and ensuring proper state management in the injection points testing framework.

## Parameters / Member Variables
- : Exit code passed by the callback mechanism (not used in implementation)
- : Datum argument passed by the callback mechanism (not used in implementation)

## Dependencies
- Functions called/Symbols referenced:
  -  (function to detach an injection point by name)
  -  (global flag indicating if local injection points exist)
  -  (global list containing locally attached injection point names)
  -  (PostgreSQL macro to extract string value from list cell)
  -  (PostgreSQL macro to get data from list cell)
- Called from:
  -  (at src/test/modules/injection_points/injection_points.c:376)

## Notes and Other Information
- This is a static function, only accessible within injection_points.c
- Designed to be registered as a  callback using PostgreSQL's exit callback mechanism
- Performs early return if  is false, optimizing for the common case where no local injection points are registered
- Uses PostgreSQL's list iteration macros (, , ) for traversing the injection point names
- The  and  parameters follow PostgreSQL's callback convention but are not used in this implementation
- Critical for proper resource cleanup in testing scenarios involving injection points