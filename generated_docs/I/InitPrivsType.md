# InitPrivsType

## Location
src/include/catalog/pg_init_privs.h: 81 - 83

## Overview
InitPrivsType is an enumeration that differentiates the source of initial privileges in PostgreSQL's privilege system, distinguishing between privileges set during database initialization (initdb) and those created by extensions.

## Definition


## Detailed Description
InitPrivsType is used within PostgreSQL's privilege management system to track the origin of initial privileges stored in the pg_init_privs catalog table. This enumeration is critical for maintaining proper privilege semantics when dealing with database initialization versus extension-created objects.

The enum serves as a type for the 'privtype' field in the pg_init_privs catalog, which stores the initial privileges for database objects. This distinction is essential because privileges granted during initdb have different semantics and lifecycle management compared to privileges established by extensions during CREATE EXTENSION operations.

This type ensures that the system can properly differentiate between core system privileges and extension-specific privileges, which is important for operations like extension drops, upgrades, and privilege management.

## Parameters / Member Variables
- : Indicates privileges were set during database initialization (initdb). Represented by the character 'i'
- : Indicates privileges were set by an extension during CREATE EXTENSION. Represented by the character 'e'

## Dependencies
- Functions called/Symbols referenced:
  - (This is an enum definition, no direct function calls)
- Called from (representative examples):
  - recordExtensionInitPriv (uses INITPRIVS_EXTENSION)
  - InternalDefaultACL (privilege management function)
  - ExecGrant_Attribute (attribute privilege granting)
  - ExecGrant_Relation (relation privilege granting)

## Notes and Other Information
- This enum is defined in pg_init_privs.h alongside the pg_init_privs catalog definition
- The enum values are single characters ('i' and 'e') to optimize storage in the catalog table
- Used primarily by the privilege management system during initdb and extension creation processes
- The pg_init_privs catalog stores initial privileges that can be restored when needed, and this enum helps identify the source context
- Extensions use recordExtensionInitPriv() function which utilizes INITPRIVS_EXTENSION to mark extension-created privileges
- Critical for proper privilege cleanup when extensions are dropped or when database objects are restored to initial privilege states