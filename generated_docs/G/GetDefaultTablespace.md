# GetDefaultTablespace

## Location
src/backend/commands/tablespace.c: 1143 - 1193

## Overview
Gets the OID of the current default tablespace for creating new database objects, with special handling for temporary objects and partitioned tables.

## Definition


## Detailed Description
This function determines the appropriate default tablespace for creating new database objects based on the relation persistence type and whether the object is partitioned. It handles three main cases:

1. **Temporary objects**: Uses a separate temporary tablespace mechanism by calling PrepareTempTablespaces() and GetNextTempTableSpace()
2. **Empty default_tablespace GUC**: Returns InvalidOid to indicate the database's default tablespace should be used
3. **Specified default_tablespace**: Looks up the tablespace OID using get_tablespace_oid() with validation

The function includes special logic to prevent specifying the database's default tablespace for partitioned tables, as this can be confusing. It also gracefully handles cases where the configured tablespace no longer exists by returning InvalidOid.

## Parameters
- : Character indicating the persistence type of the relation (RELPERSISTENCE_TEMP for temporary objects, etc.)
- : Boolean flag indicating whether the object being created is a partitioned table

## Dependencies
- Functions called/Symbols referenced:
  - [PrepareTempTablespaces](../P/PrepareTempTablespaces.md)
  - GetNextTempTableSpace
  - [get_tablespace_oid](../g/get_tablespace_oid.md)
  - RELPERSISTENCE_TEMP (constant)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:781)
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md) (src/backend/commands/matview.c:299)
  - [DefineRelation](../D/DefineRelation.md) (src/backend/commands/tablecmds.c:830)

## Notes and Other Information
- Returns InvalidOid to indicate "use the database's default tablespace"
- Caller is expected to check appropriate permissions for any non-InvalidOid result
- The function deliberately avoids caching lookups to detect dropped tablespaces
- Silently returns InvalidOid if the configured tablespace doesn't exist, rather than throwing an error
- Prevents partitioned tables from explicitly using the database's default tablespace to avoid confusion