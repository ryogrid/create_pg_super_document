# ConnectionOption

## Location
src/backend/foreign/foreign.c: 564 - 600

## Overview
Defines the structure for valid PostgreSQL foreign data wrapper connection options, mapping libpq connection parameters to their appropriate PostgreSQL catalog contexts.

## Definition


## Detailed Description
The ConnectionOption structure is used to describe valid options for PostgreSQL Foreign Data Wrapper (FDW), server, and user mapping configurations. It serves as a mapping table that associates libpq connection parameter names with the specific PostgreSQL system catalog contexts where they are allowed to appear. This structure is primarily used in the foreign data wrapper validation system to ensure that connection options are specified in the correct context (either at the server level or user mapping level).

The structure is used as part of a static array  that contains all valid libpq connection parameters copied from fe-connect.c PQconninfoOptions, along with their appropriate contexts.

## Parameters / Member Variables
- : The name of the connection option parameter (e.g., "host", "port", "dbname", "user", "password")
- : The Object ID (Oid) of the PostgreSQL system catalog relation where this option is valid (typically ForeignServerRelationId for server options or UserMappingRelationId for user-specific options)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [is_conninfo_option](../i/is_conninfo_option.md) (foreign.c:603)
  - [postgresql_fdw_validator](../p/postgresql_fdw_validator.md) (foreign.c:638)

## Notes and Other Information
- The structure is used in a static array that ends with a NULL sentinel entry {NULL, InvalidOid}
- Connection options like "user" and "password" are restricted to UserMappingRelationId context for security reasons
- Server-level options like "host", "port", "dbname" use ForeignServerRelationId context
- The list is intentionally kept small and uses linear search rather than binary search for simplicity
- This validation mechanism helps ensure proper separation of connection parameters between server and user contexts in PostgreSQL's foreign data wrapper system