# getObjectDescriptionOids

## Location
[src/backend/catalog/objectaddress.c:4071-4087](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/objectaddress.c#L4071-L4087)

## Overview
A convenience wrapper function that creates an ObjectAddress from individual OIDs and calls getObjectDescription to generate human-readable descriptions of database objects.

## Definition
```c
char *getObjectDescriptionOids(Oid classid, Oid objid)
```

## Detailed Description
This function provides a simplified interface to getObjectDescription by accepting separate class ID and object ID parameters instead of requiring the caller to construct an ObjectAddress structure. It automatically sets the objectSubId to 0 (indicating a whole object rather than a sub-object) and passes missing_ok as false (meaning it will throw an error if the object is not found). The function is essentially a thin wrapper that constructs an ObjectAddress and delegates to getObjectDescription.

## Parameters / Member Variables
- `classid`: OID of the system catalog containing the object (e.g., RelationRelationId for tables)
- `objid`: OID of the specific object within that catalog

## Dependencies
- Functions called/Symbols referenced:
  - [getObjectDescription](getObjectDescription.md) (the main implementation function)

- Called from (representative examples):
  - [AlterObjectRename_internal](../A/AlterObjectRename_internal.md) (object renaming operations)
  - [AlterObjectNamespace_internal](../A/AlterObjectNamespace_internal.md) (namespace change operations)
  - ObjectAddressSet (object address construction helper)

## Notes and Other Information
- Returns a pallocd string that must be freed by the caller