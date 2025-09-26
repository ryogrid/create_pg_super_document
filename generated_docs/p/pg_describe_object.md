# pg_describe_object

## Location
[src/backend/catalog/objectaddress.c:4205-4232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4205-L4232)

## Overview
A SQL-callable function that provides a PostgreSQL SQL interface to getObjectDescription, allowing users to retrieve human-readable descriptions of database objects from SQL queries.

## Definition
```c
Datum pg_describe_object(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the SQL-level interface to the internal getObjectDescription functionality. It accepts three arguments representing an object address (class ID, object ID, and sub-object ID) and returns a text description of the object. The function includes special handling for "pinned" dependencies in pg_depend (indicated by invalid OIDs) by returning NULL for these system-level dependencies.

The function uses missing_ok=true when calling getObjectDescription, meaning it will return NULL rather than throwing an error if the object cannot be found. This makes it suitable for use in SQL queries where missing objects should be handled gracefully. The result is converted from a C string to PostgreSQL's text type for SQL compatibility.

## Parameters / Member Variables
- Function arguments (accessed via PG_FUNCTION_ARGS):
  - `classid`: OID of the system catalog containing the object
  - `objid`: OID of the specific object within that catalog  
  - `objsubid`: Sub-object identifier (0 for whole objects, column number for table columns, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (argument extraction for OIDs)
  - PG_GETARG_INT32 (argument extraction for sub-object ID)
  - OidIsValid (OID validation)
  - [getObjectDescription](../g/getObjectDescription.md) (core description functionality)
  - [cstring_to_text](../c/cstring_to_text.md) (string conversion for SQL)
  - PG_RETURN_NULL (null return macro)
  - PG_RETURN_TEXT_P (text return macro)

- Called from:
  - SQL queries and functions (accessible as a SQL function)

## Notes and Other Information
- Exposed as a SQL function, typically called from SQL queries and pg_depend-related queries
- Returns NULL for pinned dependencies (system-level objects with invalid OIDs)
- Always uses missing_ok=true, so never throws errors for missing objects
- Converts C strings to PostgreSQL text type for SQL compatibility
- Handles the object address construction internally from separate arguments
- Useful for introspection queries and debugging dependency relationships
- Part of PostgreSQL's system administration and introspection capabilities
- Can be used in queries against pg_depend to get readable object descriptions