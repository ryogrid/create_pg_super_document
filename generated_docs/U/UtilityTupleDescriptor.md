# UtilityTupleDescriptor

## Location
[src/backend/tcop/utility.c:2082-2134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L2082-L2134)

## Overview
UtilityTupleDescriptor fetches the actual output tuple descriptor for utility statements that return tuples, providing the structure definition needed for query result processing.

## Definition

```c
TupleDesc
UtilityTupleDescriptor(Node *parsetree)
```
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
  - [CallStmtResultDesc](../C/CallStmtResultDesc.md) (for CALL statements)
  - [GetPortalByName](../G/GetPortalByName.md), PortalIsValid (for FETCH statements)
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md) (to copy portal tuple descriptors)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md), FetchPreparedStatementResultDesc (for EXECUTE statements)
  - [ExplainResultDesc](../E/ExplainResultDesc.md) (for EXPLAIN statements)
  - [GetPGVariableResultDesc](../G/GetPGVariableResultDesc.md) (for SHOW statements)

- Called from:
  - [PortalStart](../P/PortalStart.md) (during portal initialization)
  - [PlanCacheComputeResultDesc](../P/PlanCacheComputeResultDesc.md) (for result descriptor computation)
  - COMMAND_IS_NOT_READ_ONLY macro

## Notes and Other Information
- This function assumes UtilityReturnsTuples() was called first and returned true
- Memory management: The returned descriptor is allocated in the current memory context
- Error handling is minimal - returns NULL rather than raising errors for invalid operations
- MOVE operations within FETCH statements explicitly return NULL as they don't produce output
- Part of the query processing infrastructure that supports proper result formatting for utility commands

## Simplified Source

```c
// Simplified version of UtilityTupleDescriptor
TupleDesc UtilityTupleDescriptor(Node *parsetree) {
    // Dispatch to appropriate handler based on statement type
    switch (nodeTag(parsetree)) {
        case T_CallStmt:
            // Get result descriptor for function calls
            return CallStmtResultDesc((CallStmt *) parsetree);

        case T_FetchStmt:
            {
                FetchStmt *stmt = (FetchStmt *) parsetree;
                Portal portal;

                // MOVE operations don't return tuples
                if (stmt->ismove) {
                    return NULL;
                }

                // Get portal and copy its tuple descriptor
                portal = GetPortalByName(stmt->portalname);
                if (!PortalIsValid(portal)) {
                    return NULL;  // Invalid portal, don't error
                }
                return CreateTupleDescCopy(portal->tupDesc);
            }

        case T_ExecuteStmt:
            {
                ExecuteStmt *stmt = (ExecuteStmt *) parsetree;
                PreparedStatement *entry;

                // Get prepared statement and its result descriptor
                entry = FetchPreparedStatement(stmt->name, false);
                if (!entry) {
                    return NULL;  // Invalid statement, don't error
                }
                return FetchPreparedStatementResultDesc(entry);
            }

        case T_ExplainStmt:
            // Get standard EXPLAIN output format
            return ExplainResultDesc((ExplainStmt *) parsetree);

        case T_VariableShowStmt:
            {
                VariableShowStmt *stmt = (VariableShowStmt *) parsetree;
                // Get descriptor for SHOW command output
                return GetPGVariableResultDesc(stmt->name);
            }

        default:
            // Unknown utility statement type
            return NULL;
    }
}
```

Key simplifications made:
- Added clear comments explaining each statement type handler
- Simplified error handling explanation (returns NULL instead of erroring)
- Highlighted the dispatch pattern based on node type
- Focused on the main purpose: getting tuple descriptors for different utility statements
- Preserved the essential logic while making the flow clearer