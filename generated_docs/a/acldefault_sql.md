# acldefault_sql

## Location
[src/backend/utils/adt/acl.c:920-991](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L920-L991)

## Overview
SQL-accessible version of the acldefault() function that provides default ACL (Access Control List) privileges for various PostgreSQL object types.

## Definition

```c
Datum
acldefault_sql(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a SQL-callable wrapper around the internal acldefault() function. It takes a character abbreviation representing an object type and an owner OID, then returns the default ACL for that object type. The function uses a switch statement to map single-character abbreviations to PostgreSQL's internal ObjectType enumeration values, providing a bridge between SQL queries and the internal ACL system.

## Parameters / Member Variables
-  (char): Single character abbreviation representing the object type:
  - 'c': Column (OBJECT_COLUMN)
  - 'r': Table/Relation (OBJECT_TABLE) 
  - 's': Sequence (OBJECT_SEQUENCE)
  - 'd': Database (OBJECT_DATABASE)
  - 'f': Function (OBJECT_FUNCTION)
  - 'l': Language (OBJECT_LANGUAGE)
  - 'L': Large Object (OBJECT_LARGEOBJECT)
  - 'n': Schema/Namespace (OBJECT_SCHEMA)
  - 'p': Parameter ACL (OBJECT_PARAMETER_ACL)
  - 't': Tablespace (OBJECT_TABLESPACE)
  - 'F': Foreign Data Wrapper (OBJECT_FDW)
  - 'S': Foreign Server (OBJECT_FOREIGN_SERVER)
  - 'T': Type (OBJECT_TYPE)
-  (Oid): Object identifier of the owner for whom to generate default privileges

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR: PostgreSQL function argument extraction macro
  - PG_GETARG_OID: PostgreSQL OID argument extraction macro
  - [acldefault](acldefault.md): Core function that generates default ACL
  - PG_RETURN_ACL_P: PostgreSQL ACL return macro
  - ObjectType enumeration and its constants (OBJECT_COLUMN, OBJECT_TABLE, etc.)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function provides a hackish but necessary mapping from character abbreviations to internal object type constants
- Throws an ERROR if an unrecognized object type abbreviation is provided
- Part of PostgreSQL's ACL (Access Control List) system for managing privileges
- Located in src/backend/utils/adt/acl.c:920-991