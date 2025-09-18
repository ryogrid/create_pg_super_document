# session_username

## Location
src/bin/psql/common.c: 2152 - 2172

## Overview
This function retrieves the session username for the current PostgreSQL connection, returning either the session authorization user or the connection user.

## Definition


## Detailed Description
The  function is a utility function in psql that returns the username associated with the current database session. It first attempts to get the session authorization username by querying the server's  parameter. If no session authorization is set (i.e., the parameter returns NULL), it falls back to returning the connection username obtained via .

This distinction is important because PostgreSQL supports the concept of session authorization, where a user can temporarily assume the identity of another user (typically done with ). The function prioritizes the session authorization user over the connection user, providing the effective username for the current session context.

The function returns NULL if there's no active database connection.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - PQparameterStatus (libpq function to query server parameters)
  - PQuser (libpq function to get connection username)
  - pset.db (global psql database connection)
- Called from (representative examples):
  - Prompt generation functions in prompt.c
  - Various psql utility functions that need current user context

## Notes and Other Information
- This function is specific to psql client application, not the PostgreSQL backend
- The function handles the PostgreSQL security model where session authorization can override the connection user
- Session authorization is typically set using  command
- When session authorization is active, this function returns that user; otherwise, it returns the original connection user
- This is commonly used in psql prompts to display the effective username
- The returned pointer should not be freed as it points to libpq-managed memory
- Important for security context awareness in psql operations and user interface elements