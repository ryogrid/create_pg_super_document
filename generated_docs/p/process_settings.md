# process_settings

## Location
[src/backend/utils/init/postinit.c:1327-1360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L1327-L1360)

## Overview
process_settings loads and applies database and role-specific GUC configuration settings from the pg_db_role_setting system catalog in hierarchical order from most specific to most general.

## Definition

```c
static void
process_settings(Oid databaseid, Oid roleid)
```
## Detailed Description
process_settings is a static function that loads GUC (Grand Unified Configuration) settings from the pg_db_role_setting system catalog. It applies configuration parameters in a hierarchical precedence order, from most specific (database+role combination) to most general (global defaults).

The function opens the pg_db_role_setting relation and uses a single catalog snapshot for efficiency. It then calls ApplySetting four times in descending order of precedence: database+role specific settings, role-only settings, database-only settings, and finally global settings. Settings applied earlier take precedence over later ones, implementing PostgreSQL's configuration hierarchy.

The function only operates under the postmaster (not in standalone mode) and ensures proper snapshot management for catalog access.

## Parameters / Member Variables
- : OID of the database for which to load settings
- : OID of the role (user) for which to load settings

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [GetCatalogSnapshot](../G/GetCatalogSnapshot.md)
  - [RegisterSnapshot](../R/RegisterSnapshot.md)
  - [ApplySetting](../A/ApplySetting.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
  - [table_close](../t/table_close.md)
  - PGC_S_DATABASE_USER
  - PGC_S_USER
  - PGC_S_DATABASE
  - PGC_S_GLOBAL
  - DbRoleSettingRelationId
  - AccessShareLock
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)

## Notes and Other Information
- This is a static function, only callable within the same source file
- Only executes when IsUnderPostmaster is true (not in standalone backends)
- Uses a single snapshot for all catalog reads to ensure consistency
- Applies settings in strict precedence order: DATABASE_USER > USER > DATABASE > GLOBAL
- Earlier applied settings override later ones due to PostgreSQL's GUC precedence system
- Critical for implementing per-database and per-role configuration customization
- Part of PostgreSQL's flexible configuration system that allows fine-grained control over server parameters
- Uses proper locking (AccessShareLock) to ensure safe concurrent access to the system catalog