# NamespaceInfo

## Location
src/bin/pg_dump/pg_dump.h: 185 - 186

## Overview
NamespaceInfo represents a PostgreSQL schema (namespace) object in pg_dump, storing both the base dumpable object metadata and ACL information along with schema-specific properties.

## Definition


## Detailed Description
NamespaceInfo extends DumpableObjectWithAcl to represent PostgreSQL schemas (namespaces) during the dump process. It contains all the necessary information to dump and restore a schema, including its ownership, access control lists, and whether a full CREATE SCHEMA statement is needed or just ownership modification.

The structure is populated by the getNamespaces() function, which queries the pg_namespace system catalog to retrieve all namespaces in the database. Special handling is provided for the 'public' schema, which has predetermined default ACLs to maintain compatibility across PostgreSQL versions and handle ownership changes properly.

## Parameters / Member Variables
- : Base DumpableObject containing metadata like catalog ID, dump ID, name, namespace reference, dump components, and dependencies
- : DumpableAcl structure with ACL string, default ACL, privilege type, and initial privileges from pg_init_privs
- : Boolean flag indicating whether to emit CREATE SCHEMA statement (true) or just set the owner (false)
- : OID of the schema owner from pg_namespace.nspowner
- : String name of the owner role, resolved from the owner OID

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject (base structure)
  - DumpableAcl (ACL data structure)
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [getNamespaces](../g/getNamespaces.md) (creates and populates NamespaceInfo arrays)
  - [findNamespaceByOid](../f/findNamespaceByOid.md) (searches for NamespaceInfo by OID)
  - [selectDumpableNamespace](../s/selectDumpableNamespace.md) (determines if namespace should be dumped)
  - [dumpNamespace](../d/dumpNamespace.md) (outputs CREATE SCHEMA statements)
  - fmtQualifiedDumpable (formats qualified object names)

## Notes and Other Information
- Special handling for 'public' schema: pg_dump synthesizes standard v15+ ACLs for the public schema rather than using pg_init_privs entries to maintain application compatibility
- The 'create' flag helps distinguish between full schema creation and ownership-only operations during restore
- Used throughout pg_dump to maintain namespace context for other database objects
- Inherits ACL handling capabilities from DumpableObjectWithAcl pattern
- Located in src/bin/pg_dump/pg_dump.h:178-185