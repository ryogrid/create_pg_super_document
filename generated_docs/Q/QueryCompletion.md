# QueryCompletion

## Location
[src/include/tcop/cmdtag.h:29-33](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/cmdtag.h#L29-L33)

## Overview
A structure that holds information about the completion status of an executed SQL command, including the command type and the number of rows processed.

## Definition
```c
typedef struct QueryCompletion
{
    CommandTag  commandTag;
    uint64      nprocessed;
} QueryCompletion;
```

## Detailed Description
QueryCompletion is a fundamental data structure in PostgreSQL that encapsulates the result information from executing SQL commands. It serves as a standardized way to communicate command execution results throughout the system and back to client applications.

The structure contains two essential pieces of information: the type of command that was executed (via CommandTag enum) and the count of rows that were affected or processed by that command. This information is crucial for client applications to understand what happened during command execution and is typically included in command completion messages sent to clients.

This structure is used extensively throughout the PostgreSQL codebase, from low-level command execution to high-level portal management and client communication.

## Parameters / Member Variables
- `commandTag`: A CommandTag enum value that identifies the specific type of SQL command that was executed (e.g., SELECT, INSERT, UPDATE, DELETE, etc.)
- `nprocessed`: A 64-bit unsigned integer representing the number of rows that were processed, affected, or returned by the command

## Dependencies
- Functions called/Symbols referenced:
  - CommandTag (enum type)
- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExecRefreshMatView](../E/ExecRefreshMatView.md)
  - [PerformPortalFetch](../P/PerformPortalFetch.md)
  - [ExecuteQuery](../E/ExecuteQuery.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [ProcessQuery](../P/ProcessQuery.md)
  - [PortalRun](../P/PortalRun.md)
  - [ProcessUtility](../P/ProcessUtility.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [SetQueryCompletion](../S/SetQueryCompletion.md)
  - [CopyQueryCompletion](../C/CopyQueryCompletion.md)
  - [InitializeQueryCompletion](../I/InitializeQueryCompletion.md)
  - [BuildQueryCompletionString](../B/BuildQueryCompletionString.md)

## Notes and Other Information
- This structure is defined in src/include/tcop/cmdtag.h
- The structure is used across multiple subsystems including executor, planner, utility commands, and client communication
- The nprocessed field is particularly meaningful for DML commands (INSERT, UPDATE, DELETE) but may also be used for other command types
- [QueryCompletion](QueryCompletion.md) structures are often embedded in other structures like PortalData and PreparedStatement
- The structure works in conjunction with utility functions like SetQueryCompletion and CopyQueryCompletion for easy manipulation
- Client applications receive this information in the form of completion tags in the PostgreSQL wire protocol
- The CommandTag enum referenced by this structure is generated from tcop/cmdtaglist.h