# DropSetting

## Location
[src/backend/catalog/pg_db_role_setting.c:170-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_db_role_setting.c#L170-L219)

## Overview
DropSetting removes configuration parameter settings from the pg_db_role_setting catalog when databases or roles are dropped from the system.

## Definition
```c
void DropSetting(Oid databaseid, Oid roleid)
```

## Detailed Description
DropSetting performs cleanup operations on the pg_db_role_setting catalog table by removing configuration parameter entries associated with specific databases or roles that are being dropped. The function supports flexible deletion patterns:

1. **Database-specific cleanup**: When a valid databaseid is provided, it removes all settings associated with that database (across all roles)
2. **Role-specific cleanup**: When a valid roleid is provided, it removes all settings associated with that role (across all databases)
3. **Combined cleanup**: Both parameters can be valid simultaneously, though this is not typical in current PostgreSQL usage

The function performs a catalog table scan using the specified criteria and deletes all matching tuples. It uses a sequential scan rather than an index scan to ensure all matching entries are found and removed.

## Parameters / Member Variables
- `databaseid`: OID of the database whose settings should be dropped (InvalidOid to ignore database filtering)
- `roleid`: OID of the role whose settings should be dropped (InvalidOid to ignore role filtering)

## Dependencies
- Functions called/Symbols referenced:
  - table_open/table_close
  - OidIsValid
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)/table_endscan
  - [heap_getnext](../h/heap_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - ForwardScanDirection
- Called from (representative examples):
  - [dropdb](../d/dropdb.md) (src/backend/commands/dbcommands.c:1767)
  - [DropRole](DropRole.md) (src/backend/commands/user.c:1320)

## Notes and Other Information
- Acquires RowExclusiveLock on pg_db_role_setting to ensure exclusive access during cleanup operations
- Uses table_beginscan_catalog for efficient catalog scanning with appropriate visibility rules
- Performs bulk deletion by scanning and deleting all matching tuples in a single transaction
- Essential for maintaining referential integrity when databases or roles are dropped
- The function is called during DROP DATABASE and DROP ROLE operations to prevent orphaned configuration entries
- Scan keys are built dynamically based on which OID parameters are valid, allowing flexible deletion patterns