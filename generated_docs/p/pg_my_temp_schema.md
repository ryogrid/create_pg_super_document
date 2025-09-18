# pg_my_temp_schema

## Location
[src/backend/catalog/namespace.c:5076-5081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L5076-L5081)

## Overview
A PostgreSQL system function that returns the OID of the current session's temporary schema namespace.

## Definition


## Detailed Description
This function provides access to the current session's temporary schema namespace by returning the OID of the temporary namespace. In PostgreSQL, each session can have its own temporary schema where temporary tables, views, and other temporary objects are created. This function returns the OID of that namespace if it exists, or InvalidOid if no temporary schema has been created for the current session yet.

The function directly returns the value of the global variable , which tracks the temporary namespace for the current backend process. This namespace is created on-demand when the first temporary object is created in a session.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure (no arguments used)

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for returning OID result)
  -  (global variable storing the temporary namespace OID)
- Called from:
  - Available as SQL system function 

## Notes and Other Information
- Returns the OID of the current session's temporary namespace, or InvalidOid if none exists
- The temporary namespace is created lazily when the first temporary object is needed
- Each database session has its own separate temporary namespace
- Located in 
- This is one of the simplest namespace-related system functions, directly exposing an internal state variable
- The  variable is initialized to InvalidOid and remains so until a temporary schema is actually needed