# ExecStatus

## Location
[src/backend/executor/functions.c:63-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L63-L64)

## Overview
ExecStatus is an enumeration type that tracks the execution state of SQL function statements within PostgreSQL's function execution framework.

## Definition


## Detailed Description
ExecStatus is used to manage the lifecycle of SQL function execution within the PostgreSQL executor. It provides a simple state machine for tracking whether a particular execution state record is starting, currently running, or has completed execution. This enumeration is part of the SQL function execution infrastructure that handles the execution of user-defined SQL functions.

The enum values represent the three primary phases of function execution:
- **F_EXEC_START**: Initial state before execution begins
- **F_EXEC_RUN**: Active execution state where the query is being processed  
- **F_EXEC_DONE**: Completed execution state

## Parameters / Member Variables
- : Indicates the execution state is initialized but not yet running
- : Indicates the execution state is actively running a query
- : Indicates the execution state has completed processing

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a basic enumeration type)
- Used by:
  - [execution_state](../e/execution_state.md) struct (as the status field at src/backend/executor/functions.c:68)

## Notes and Other Information
- Defined in src/backend/executor/functions.c:60-63
- This enumeration is specifically designed for the SQL function execution framework
- Each execution_state record maintains one of these status values to coordinate proper execution flow
- The status transitions typically follow the sequence: F_EXEC_START → F_EXEC_RUN → F_EXEC_DONE