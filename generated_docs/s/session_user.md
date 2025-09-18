# session_user

## Location
[src/backend/utils/adt/name.c:269-278](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L269-L278)

## Overview
The session_user function is a SQL function that returns the name of the user who initiated the current database session.

## Definition


## Detailed Description
This function implements the SQL standard SESSION_USER function. It retrieves the session user ID and converts it to the corresponding username string. Unlike current_user which can change due to SET ROLE or SET SESSION AUTHORIZATION, session_user always returns the original authenticated user who established the database session.

The function follows PostgreSQL's function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum type that can be used within SQL queries.

## Parameters / Member Variables
- This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL's standard function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [GetSessionUserId](../G/GetSessionUserId.md): Retrieves the session user's OID (different from current user ID)
  - [GetUserNameFromId](../G/GetUserNameFromId.md): Converts user OID to username string
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - DirectFunctionCall1: Directly calls a PostgreSQL function with one argument
  - namein: Input function for the name data type
  - PG_RETURN_DATUM: Macro to return a Datum from a PostgreSQL function

- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md): Used in expression evaluation

## Notes and Other Information
- This function is part of the SQL standard and provides session information
- Key difference from current_user: session_user is immutable during a session, while current_user can change with role switching
- The function is defined in src/backend/utils/adt/name.c alongside other name-related functions
- Returns the name as PostgreSQL's 'name' data type
- Essential for security and auditing purposes as it always identifies the original authenticating user