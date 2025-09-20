# current_user

## Location
[src/backend/utils/adt/name.c:263-268](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/name.c#L263-L268)

## Overview
The current_user function is a SQL function that returns the name of the current user who is executing the statement.

## Definition

```c
Datum
current_user(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the SQL standard CURRENT_USER function. It retrieves the user ID of the currently executing session and converts it to the corresponding username string. The function uses PostgreSQL's internal user identification system to return the authenticated user's name as a SQL datum.

The function follows PostgreSQL's function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum type that can be used within SQL queries.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [GetUserId](../G/GetUserId.md): Retrieves the current user's OID
  - [GetUserNameFromId](../G/GetUserNameFromId.md): Converts user OID to username string
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - DirectFunctionCall1: Directly calls a PostgreSQL function with one argument
  - namein: Input function for the name data type
  - PG_RETURN_DATUM: Macro to return a Datum from a PostgreSQL function

- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md): Used in expression evaluation

## Notes and Other Information
- This function is part of the SQL standard and provides session information
- It works in conjunction with session_user and other user identification functions
- The function is defined in src/backend/utils/adt/name.c alongside other name-related functions
- Returns the name as PostgreSQL's 'name' data type, which has specific length limitations