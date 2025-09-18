# ExecEvalMergeSupportFunc

## Location
[src/backend/executor/execExprInterp.c:4716-4752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L4716-L4752)

## Overview
ExecEvalMergeSupportFunc provides runtime information about the current MERGE action being executed, returning the action type as a text value for use in RETURNING clauses.

## Definition
void ExecEvalMergeSupportFunc(ExprState *state, ExprEvalStep *op, ExprContext *econtext)

## Detailed Description
This function supports the SQL MERGE statement by providing access to metadata about the currently executing MERGE action. It's primarily used in RETURNING clauses to allow queries to determine which type of action (INSERT, UPDATE, or DELETE) was performed on each row during MERGE execution.

The function examines the current ModifyTableState to determine which MergeActionState is active, then inspects the command type of that action to return the appropriate string value. It converts the internal PostgreSQL command type constants (CMD_INSERT, CMD_UPDATE, CMD_DELETE) into their corresponding SQL standard text representations.

This enables SQL queries like:


## Parameters / Member Variables
- : The ExprState containing the expression evaluation context, with parent pointing to the ModifyTableState
- : The ExprEvalStep operation descriptor containing result storage pointers
- : The ExprContext for expression evaluation (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - elog
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - cstring_to_text_with_len
  - CMD_INSERT, CMD_UPDATE, CMD_DELETE, CMD_NOTHING (command type constants)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (main expression interpreter loop)

## Notes and Other Information
- This function is only valid within MERGE statement execution contexts
- Returns text values "INSERT", "UPDATE", or "DELETE" corresponding to the current action
- The function will error if called outside of an active MERGE operation
- Part of PostgreSQL's SQL MERGE statement implementation (SQL:2003 standard)
- Enables introspection of MERGE actions in RETURNING clauses
- The CMD_NOTHING case is treated as an error since it represents a planning-time concept