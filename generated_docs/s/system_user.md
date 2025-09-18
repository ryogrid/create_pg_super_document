# system_user

## Location
src/backend/utils/init/miscinit.c: 944 - 965

## Overview
The system_user function is a PostgreSQL SQL function that returns the system user name associated with the current database session.

## Definition


## Detailed Description
The system_user function implements the SQL SYSTEM_USER function in PostgreSQL. It retrieves the system user name through the GetSystemUser() function and returns it as a PostgreSQL text datum. If no system user is available, the function returns NULL. This function is part of PostgreSQL's security and session management infrastructure, providing information about the authenticated system user for the current session.

## Parameters / Member Variables
- This function follows PostgreSQL's standard function calling convention using PG_FUNCTION_ARGS, which contains the function arguments passed from the SQL layer.

## Dependencies
- Functions called/Symbols referenced:
  - [GetSystemUser](../G/GetSystemUser.md)
  - PG_RETURN_DATUM
  - CStringGetTextDatum
  - PG_RETURN_NULL
- Called from (representative examples):
  - [parse_ident_line](../p/parse_ident_line.md) (in HBA authentication)
  - [check_ident_usermap](../c/check_ident_usermap.md) (in identity mapping)
  - [InitializeSystemUser](../I/InitializeSystemUser.md) (during system user initialization)

## Notes and Other Information
- This function is primarily used in PostgreSQL's authentication and authorization system
- It's commonly referenced in HBA (Host-Based Authentication) processing for identity mapping
- The function may return NULL if no system user information is available
- It serves as the backend implementation for the SQL SYSTEM_USER function that can be called from SQL queries