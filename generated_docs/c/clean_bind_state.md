# clean_bind_state

## Location
[src/bin/psql/common.c:2255-2277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L2255-L2277)

## Overview
Resets and cleans up state related to the \bind command in psql, freeing allocated memory for bind parameters and resetting bind flags.

## Definition


## Detailed Description
The  function is responsible for cleaning up any state related to bind parameters and the bind flag in the psql client. This function must be called after processing a query or when running the  command to prevent memory leaks and ensure proper state management.

When bind parameters are active (indicated by  being true), the function iterates through all stored bind parameters, freeing each individual parameter string, then frees the array of parameter pointers itself. Finally, it resets the bind state by setting the parameter array pointer to NULL and the bind flag to false.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library function)
  - pset.bind_flag (global variable)
  - pset.bind_params (global variable)
  - pset.bind_nparams (global variable)
- Called from (representative examples):
  - [exec_command_bind](../e/exec_command_bind.md) (src/bin/psql/command.c:495)
  - [SendQuery](../S/SendQuery.md) (src/bin/psql/common.c:1278)

## Notes and Other Information
- This function is part of the psql client's parameter binding mechanism
- It's essential for preventing memory leaks when using parameterized queries
- The function safely handles the case where no bind parameters are currently set
- Located in src/bin/psql/common.c:2255-2277