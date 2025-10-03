# CheckAlterPublication

## Location
[src/backend/commands/publicationcmds.c:1333-1370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/publicationcmds.c#L1333-L1370)

## Overview
CheckAlterPublication validates whether relations and schemas can be legally added to or modified in a publication, enforcing permission and consistency constraints.

## Definition

```c
static void
CheckAlterPublication(AlterPublicationStmt *stmt, HeapTuple tup,
					  List *tables, List *schemaidlist)
```
## Detailed Description
This internal validation function performs comprehensive checks before allowing publication alterations. It enforces PostgreSQL's publication security model by requiring superuser privileges for schema operations and prevents logical conflicts by blocking table/schema additions to FOR ALL TABLES publications. The function acts as a gatekeeper to ensure publication modifications maintain system integrity and follow access control policies.

## Parameters / Member Variables
- `*stmt`: AlterPublicationStmt pointer containing the publication alteration statement with action type (AP_AddObjects, AP_SetObjects, etc.)
- `tup`: HeapTuple representing the existing publication record from pg_publication catalog
- `*tables`: List of table OIDs to be added/modified in the publication (can be NULL)
- `*schemaidlist`: List of schema OIDs to be added/modified in the publication (can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [superuser](../s/superuser.md) (privilege checking)
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

## Simplified Source

```c
static void CheckAlterPublication(AlterPublicationStmt *stmt, HeapTuple tup,
                                 List *tables, List *schemaidlist) {
    Form_pg_publication pubform = (Form_pg_publication) GETSTRUCT(tup);

    // Check superuser privilege for schema operations
    if ((stmt->action == AP_AddObjects || stmt->action == AP_SetObjects) &&
        schemaidlist && !superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("must be superuser to add or set schemas")));
    }

    // Prevent schema operations on FOR ALL TABLES publications
    if (schemaidlist && pubform->puballtables) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("publication \"%s\" is defined as FOR ALL TABLES",
                              NameStr(pubform->pubname)),
                       errdetail("Schemas cannot be added to or dropped from FOR ALL TABLES publications.")));
    }

    // Prevent table operations on FOR ALL TABLES publications
    if (tables && pubform->puballtables) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                       errmsg("publication \"%s\" is defined as FOR ALL TABLES",
                              NameStr(pubform->pubname)),
                       errdetail("Tables cannot be added to or dropped from FOR ALL TABLES publications.")));
    }
}
```