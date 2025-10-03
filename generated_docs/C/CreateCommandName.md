# CreateCommandName

## Location
[src/include/tcop/utility.h:103-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tcop/utility.h#L103-L112)

## Overview
CreateCommandName is a static inline utility function that returns the human-readable name of a SQL command by converting a parse tree node to its corresponding command tag string.

## Definition

```c
static inline const char *
CreateCommandName(Node *parsetree)
```
## Detailed Description
CreateCommandName serves as a convenience function that combines two operations: first, it determines the CommandTag for a given parse tree node using CreateCommandTag(), then it converts that tag to its human-readable string representation using GetCommandTagName(). This function is commonly used in PostgreSQL's event trigger system, logging mechanisms, and read-only transaction checks where the system needs to identify and display the type of SQL command being executed.

The function is defined as a static inline function in the utility.h header file, making it efficient for frequent use throughout the codebase while keeping the implementation details encapsulated.

## Parameters / Member Variables
- `*parsetree`: A Node pointer representing the parse tree of a SQL statement. This can be any type of statement node (raw statements, planned statements, queries, etc.) that PostgreSQL recognizes.
## Dependencies
- Functions called/Symbols referenced:
  - [GetCommandTagName](../G/GetCommandTagName.md) - Converts a CommandTag enum to its string representation
  - [CreateCommandTag](CreateCommandTag.md) - Determines the CommandTag for a parse tree node
- Called from (representative examples):
  - [pg_event_trigger_ddl_commands](../p/pg_event_trigger_ddl_commands.md) - For DDL event trigger functionality
  - [ExecCheckXactReadOnly](../E/ExecCheckXactReadOnly.md) - For read-only transaction validation
  - [init_execution_state](../i/init_execution_state.md) - During function execution state initialization
  - [SPI_cursor_open_internal](../S/SPI_cursor_open_internal.md) - In the Server Programming Interface
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md) - During SPI plan execution

## Notes and Other Information
- This function is part of PostgreSQL's command identification infrastructure
- The function relies heavily on CreateCommandTag(), which contains an extensive switch statement covering all PostgreSQL command types
- It's frequently used in contexts where command names need to be displayed to users or logged
- The inline nature of this function makes it very efficient for the frequent command name lookups required by PostgreSQL's internal systems
- Located in src/include/tcop/utility.h at lines 102-106

## Simplified Source

```c
// Simple utility function that gets command name from parse tree
static inline const char *
CreateCommandName(Node *parsetree)
{
    return GetCommandTagName(CreateCommandTag(parsetree));
}
```