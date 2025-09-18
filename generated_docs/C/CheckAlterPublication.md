# CheckAlterPublication

## Location
src/backend/commands/publicationcmds.c: 1333 - 1370

## Overview
CheckAlterPublication validates whether relations and schemas can be legally added to or modified in a publication, enforcing permission and consistency constraints.

## Definition


## Detailed Description
This internal validation function performs comprehensive checks before allowing publication alterations. It enforces PostgreSQL's publication security model by requiring superuser privileges for schema operations and prevents logical conflicts by blocking table/schema additions to FOR ALL TABLES publications. The function acts as a gatekeeper to ensure publication modifications maintain system integrity and follow access control policies.

## Parameters / Member Variables
- : AlterPublicationStmt pointer containing the publication alteration statement with action type (AP_AddObjects, AP_SetObjects, etc.)
- : HeapTuple representing the existing publication record from pg_publication catalog
- : List of table OIDs to be added/modified in the publication (can be NULL)
- : List of schema OIDs to be added/modified in the publication (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - superuser (privilege checking)
  - ereport (error reporting)
  - GETSTRUCT (tuple data extraction)
  - NameStr (name string extraction)
- Called from (representative examples):
  - [AlterPublication](../A/AlterPublication.md)

## Notes and Other Information
- Only superusers can add or set schemas in publications due to security implications
- FOR ALL TABLES publications cannot have individual tables or schemas added/removed
- Function uses Form_pg_publication to access publication catalog data
- Error codes used: ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
- Part of PostgreSQL's logical replication publication management system