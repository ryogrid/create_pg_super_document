# DumpableObjectWithAcl

## Location
[src/bin/pg_dump/pg_dump.h:176-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L176-L177)

## Overview
DumpableObjectWithAcl is a generic struct that provides a unified interface for accessing any database object type that has Access Control Lists (ACLs) in pg_dump.

## Definition


## Detailed Description
DumpableObjectWithAcl serves as a composite structure that combines the base DumpableObject functionality with ACL-specific data handling. This structure is designed as a casting target for database objects that have ACL information, allowing pg_dump to handle ACL processing in a type-safe and uniform manner across different object types like namespaces, types, functions, aggregates, tables, procedural languages, foreign data wrappers, and foreign servers.

The structure follows PostgreSQL's pattern of embedding base structures at the beginning of derived structures, which enables safe type casting. Objects that inherit from this structure can be cast to DumpableObjectWithAcl to access both the standard dumpable object metadata and the ACL-specific information.

## Parameters / Member Variables
- : Base DumpableObject containing standard metadata like catalog ID, dump ID, name, namespace, dump components, dependencies, and extension membership information
- : DumpableAcl structure containing ACL-specific data including the actual ACL string, default ACL, privilege type, and initial privileges from pg_init_privs

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (ACL data structure)
- Called from (representative examples):
  - [getAdditionalACLs](../g/getAdditionalACLs.md) (casts objects to this type for ACL processing)

## Notes and Other Information
- This structure is used as a casting target rather than being directly instantiated
- Objects that use this pattern include NamespaceInfo, TypeInfo, ExtensionInfo, and other ACL-bearing database objects
- The casting is performed in getAdditionalACLs when processing pg_init_privs entries to set initial privilege information
- The structure layout ensures that any object with ACLs can be safely cast to DumpableObjectWithAcl for uniform ACL handling
- Located in src/bin/pg_dump/pg_dump.h:172-176