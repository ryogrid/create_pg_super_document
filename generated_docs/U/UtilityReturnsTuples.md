# UtilityReturnsTuples

## Location
src/backend/tcop/utility.c: 2026 - 2081

## Overview
UtilityReturnsTuples determines whether a utility statement will send output tuples to the destination, helping the query execution engine decide on the appropriate portal strategy.

## Definition

```c
bool
UtilityReturnsTuples(Node *parsetree)
```
## Detailed Description
UtilityReturnsTuples analyzes a utility statement's parse tree to determine if the statement will produce output tuples that need to be sent to the client. This function is crucial for the portal strategy selection process, as it helps distinguish between statements that produce tabular output (requiring tuple processing) and those that don't.

The function examines specific utility statement types:
- **CallStmt**: Returns true if the called function returns a RECORD type
- **FetchStmt**: Returns true if it's not a MOVE statement and the associated portal has a valid tuple descriptor
- **ExecuteStmt**: Returns true if the prepared statement has a result descriptor
- **ExplainStmt**: Always returns true as EXPLAIN produces tabular output
- **VariableShowStmt**: Always returns true as SHOW commands produce output
- **All other utilities**: Return false (no tuple output)

## Parameters / Member Variables
- : Pointer to the parsed utility statement node to be analyzed

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to identify the statement type)
  - GetPortalByName (for FETCH statements)
  - PortalIsValid (to validate portal existence)
  - FetchPreparedStatement (for EXECUTE statements)
  - Statement types: CallStmt, FetchStmt, ExecuteStmt, Portal, PreparedStatement

- Called from:
  - ChoosePortalStrategy (to determine portal execution strategy)
  - QueryReturnsTuples (as part of broader query analysis)
  - COMMAND_IS_NOT_READ_ONLY macro

## Notes and Other Information
- This function should have corresponding cases for each utility statement type that passes a destination in ProcessUtility
- For FETCH statements, MOVE operations are explicitly excluded as they don't return data
- Error handling is deliberately minimal - the function returns false rather than raising errors for invalid portals or prepared statements
- Part of the query processing infrastructure that helps optimize execution strategies based on output requirements