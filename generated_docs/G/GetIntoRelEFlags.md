# GetIntoRelEFlags

## Location
[src/backend/commands/createas.c:368-385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L368-L385)

## Overview
Utility function that computes executor flags needed for CREATE TABLE AS operations, particularly handling the WITH NO DATA option for query execution control.

## Definition

```c
int
GetIntoRelEFlags(IntoClause *intoClause)
```
## Detailed Description
The  function is a simple utility that translates  options into appropriate executor flags for CREATE TABLE AS operations. Currently, it primarily handles the  flag by setting the  executor flag when data population should be skipped.

This function provides a centralized point for mapping  semantics to executor behavior, ensuring consistent flag handling across different parts of the system that need to execute or analyze CREATE TABLE AS statements.

## Parameters / Member Variables
- : IntoClause structure containing the options for the CREATE TABLE AS operation, particularly the  field

## Dependencies
- Functions called/Symbols referenced:
  - EXEC_FLAG_WITH_NO_DATA
- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [ExecuteQuery](../E/ExecuteQuery.md)

## Notes and Other Information
- This is a public function exported through createas.h for use by other subsystems
- Used by EXPLAIN and PREPARE commands in addition to the main execution path
- Provides abstraction layer between IntoClause semantics and executor flags
- Currently handles only the skipData flag but designed to accommodate additional flags if needed
- Callers still need to handle skipData logic explicitly for their specific execution suppression methods
- Simple but important for maintaining consistency across different execution contexts

## Simplified Source

```c
int GetIntoRelEFlags(IntoClause *intoClause) {
    int flags = 0;

    // Set flag to skip data population if WITH NO DATA specified
    if (intoClause->skipData) {
        flags |= EXEC_FLAG_WITH_NO_DATA;
    }

    return flags;
}
```