# UtilityTupleDescriptor

## Location
src/backend/tcop/utility.c: 2082 - 2134

## Overview
UtilityTupleDescriptor fetches the actual output tuple descriptor for utility statements that return tuples, providing the structure definition needed for query result processing.

## Definition


## Detailed Description
UtilityTupleDescriptor creates and returns a tuple descriptor that describes the structure of the output tuples for utility statements. This function is only called for statements where UtilityReturnsTuples() previously returned true. The function provides specific handling for each type of tuple-returning utility statement:

- **CallStmt**: Delegates to CallStmtResultDesc() to get the function's result descriptor
- **FetchStmt**: Retrieves the portal's tuple descriptor and creates a copy using CreateTupleDescCopy()
- **ExecuteStmt**: Gets the result descriptor from the prepared statement via FetchPreparedStatementResultDesc()
- **ExplainStmt**: Uses ExplainResultDesc() to get the standard EXPLAIN output format
- **VariableShowStmt**: Calls GetPGVariableResultDesc() to get the descriptor for SHOW command output

The returned tuple descriptor is created in or copied into the current memory context, ensuring proper memory management.

## Parameters / Member Variables
- : Pointer to the parsed utility statement node for which to fetch the tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to identify statement type)
  - CallStmtResultDesc (for CALL statements)
  - GetPortalByName, PortalIsValid (for FETCH statements)
  - CreateTupleDescCopy (to copy portal tuple descriptors)
  - FetchPreparedStatement, FetchPreparedStatementResultDesc (for EXECUTE statements)
  - ExplainResultDesc (for EXPLAIN statements)
  - GetPGVariableResultDesc (for SHOW statements)

- Called from:
  - PortalStart (during portal initialization)
  - PlanCacheComputeResultDesc (for result descriptor computation)
  - COMMAND_IS_NOT_READ_ONLY macro

## Notes and Other Information
- This function assumes UtilityReturnsTuples() was called first and returned true
- Memory management: The returned descriptor is allocated in the current memory context
- Error handling is minimal - returns NULL rather than raising errors for invalid operations
- MOVE operations within FETCH statements explicitly return NULL as they don't produce output
- Part of the query processing infrastructure that supports proper result formatting for utility commands