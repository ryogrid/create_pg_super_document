# getAdditionalACLs

## Location
[src/bin/pg_dump/pg_dump.c:10017-10145](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10017-L10145)

## Overview
Collects additional ACL-related information for database objects that was not captured during initial object collection, including per-column ACLs and initial privileges from pg_init_privs catalog.

## Definition


## Detailed Description
This function performs post-processing to gather ACL information that requires all DumpableObjects to be created first. It operates in two main phases:

1. **Column ACL Detection**: Queries pg_attribute to find tables with column-level ACLs and marks the corresponding TableInfo objects with the DUMP_COMPONENT_ACL flag and hascolumnACLs flag.

2. **Initial Privileges Collection**: For PostgreSQL 9.6+, reads the pg_init_privs catalog to collect initial privilege information for various database objects and stores this data in the corresponding DumpableObjectWithAcl structures.

The function ensures that objects with column-level privileges or initial privileges are properly flagged for ACL dumping during the backup process.

## Parameters / Member Variables
- : Archive context containing connection information and version details for the database being dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [findTableByOid](../f/findTableByOid.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - pg_log_warning
  - [pstrdup](../p/pstrdup.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)

## Notes and Other Information
- Only processes pg_init_privs data for PostgreSQL 9.6 and later versions
- Skips pg_init_privs entries for the "public" schema as explained in getNamespaces()
- Supports ACL collection for specific object types: namespaces, types, functions, aggregates, tables, procedural languages, foreign data wrappers, and foreign servers
- Issues warnings for unsupported pg_init_privs entries
- Does not store actual column ACL data but only marks tables as having column ACLs