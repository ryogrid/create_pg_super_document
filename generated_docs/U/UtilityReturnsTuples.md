# UtilityReturnsTuples

## Location
[src/backend/tcop/utility.c:2026-2081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/utility.c#L2026-L2081)

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
- `*parsetree`: Pointer to the parsed utility statement node to be analyzed
## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to identify the statement type)
  - [GetPortalByName](../G/GetPortalByName.md) (for FETCH statements)
  - PortalIsValid (to validate portal existence)
  - [FetchPreparedStatement](../F/FetchPreparedStatement.md) (for EXECUTE statements)
  - Statement types: CallStmt, FetchStmt, ExecuteStmt, Portal, PreparedStatement

- Called from:
  - [ChoosePortalStrategy](../C/ChoosePortalStrategy.md) (to determine portal execution strategy)
  - [QueryReturnsTuples](../Q/QueryReturnsTuples.md) (as part of broader query analysis)
  - COMMAND_IS_NOT_READ_ONLY macro

## Notes and Other Information
- This function should have corresponding cases for each utility statement type that passes a destination in ProcessUtility
- For FETCH statements, MOVE operations are explicitly excluded as they don't return data
- Error handling is deliberately minimal - the function returns false rather than raising errors for invalid portals or prepared statements
- Part of the query processing infrastructure that helps optimize execution strategies based on output requirements

## Simplified Source

```c
// Simplified version of UtilityReturnsTuples
bool UtilityReturnsTuples(Node *parsetree) {
    // Check statement type and determine if it returns tuples
    switch (nodeTag(parsetree)) {
        case T_CallStmt:
            // Function calls return tuples if result type is RECORD
            return (((CallStmt *) parsetree)->funcexpr->funcresulttype == RECORDOID);

        case T_FetchStmt:
            // FETCH returns tuples if not a MOVE and portal is valid
            if (((FetchStmt *) parsetree)->ismove)
                return false;
            Portal portal = GetPortalByName(((FetchStmt *) parsetree)->portalname);
            return (PortalIsValid(portal) && portal->tupDesc);

        case T_ExecuteStmt:
            // EXECUTE returns tuples if prepared statement has result descriptor
            PreparedStatement *entry = FetchPreparedStatement(((ExecuteStmt *) parsetree)->name, false);
            return (entry && entry->plansource->resultDesc);

        case T_ExplainStmt:
        case T_VariableShowStmt:
            // EXPLAIN and SHOW always return tabular output
            return true;

        default:
            // All other utility statements don't return tuples
            return false;
    }
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated variable declarations with usage
- Added descriptive comments for each case
- Simplified conditional logic in FETCH case
- Combined similar cases (EXPLAIN and SHOW) with shared comment
- Removed redundant null checks that weren't in critical path