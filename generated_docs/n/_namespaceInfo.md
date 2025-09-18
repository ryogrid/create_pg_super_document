# _namespaceInfo

## Location
[src/bin/pg_dump/pg_dump.h:178-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L178-L184)

## Overview
The  structure represents PostgreSQL schemas (namespaces) in pg_dump, containing schema-specific metadata including ownership information and creation requirements along with the standard dumpable object properties.

## Definition


## Detailed Description
The  structure represents PostgreSQL schemas (also known as namespaces) in the pg_dump framework. It extends the base  with  to support schema permissions, and adds schema-specific attributes related to ownership and creation requirements.

This structure tracks not only the basic object information but also whether the schema needs to be explicitly created or if only ownership changes are required. This distinction is important because some schemas (like 'public') may already exist in the target database and only need ownership adjustments, while others need full creation statements.

## Parameters / Member Variables
- : Base  structure containing core metadata, identification, dependencies, and component control
- :  structure containing ACL information for schema permissions and access control
- : Boolean flag indicating whether to generate a CREATE SCHEMA statement (true) or just set the owner (false)
- : Object identifier (OID) of the schema owner from the source database
- : String containing the name of the role that owns this schema

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - DumpableAcl
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - Referenced by other dumpable objects that belong to specific schemas
  - Used in schema creation and ownership management functions
  - Referenced in _dumpableObject.namespace field

## Notes and Other Information
Schemas in PostgreSQL serve as containers for database objects, providing namespace isolation and access control. The  flag is particularly important for handling system schemas that may pre-exist in target databases. The structure follows the standard ACL-enabled object pattern with  immediately following . Schema ownership is tracked both by OID and role name to handle cases where role names might change between source and target databases during restoration.