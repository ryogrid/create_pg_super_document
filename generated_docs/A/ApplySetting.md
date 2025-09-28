# ApplySetting

## Location
[src/backend/catalog/pg_db_role_setting.c:220-261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_db_role_setting.c#L220-L261)

## Overview
ApplySetting loads and applies database and role-specific configuration parameter settings from the pg_db_role_setting catalog into the current PostgreSQL process.

## Definition
```c
void ApplySetting(Snapshot snapshot, Oid databaseid, Oid roleid, Relation relsetting, GucSource source)
```

## Detailed Description
ApplySetting is responsible for applying stored configuration parameters to the current PostgreSQL backend process during session initialization. The function searches the pg_db_role_setting catalog for entries matching the exact database and role combination, then loads and applies the found configuration arrays.

The function operates at a specific transaction snapshot to ensure consistency and processes configuration settings at the SUSET (superuser-settable) privilege level. This design assumes that the right to insert settings into pg_db_role_setting was validated at insertion time, allowing the settings to be applied with elevated privileges during session startup.

Key characteristics:
- Searches for exact databaseid/roleid matches only
- Processes all settings at SUSET privilege level
- Uses ProcessGUCArray to apply configuration arrays
- Operates within a specific transaction snapshot for consistency
- Designed to be called multiple times with different combinations (including InvalidOid for database/role-wide settings)

## Parameters / Member Variables
- `snapshot`: Transaction snapshot to use for catalog access, ensuring consistent view of settings
- `databaseid`: OID of the target database (InvalidOid for role-only settings)
- `roleid`: OID of the target role (InvalidOid for database-only settings)
- `relsetting`: Pre-opened pg_db_role_setting relation (must be opened with appropriate lock)
- `source`: GucSource indicating the origin of these settings (typically for audit/logging purposes)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetArrayTypeP
  - [ProcessGUCArray](../P/ProcessGUCArray.md)
  - PGC_SUSET (privilege level)
  - GUC_ACTION_SET (action type)
- Called from (representative examples):
  - [process_settings](../p/process_settings.md) (src/backend/utils/init/postinit.c:1341-1344) - called multiple times with different database/role combinations

## Notes and Other Information
- Requires the pg_db_role_setting relation to be pre-opened with appropriate locking by the caller
- Uses the DbRoleSettingDatidRolidIndexId index for efficient tuple lookup
- Processes settings at SUSET privilege level, assuming insertion-time privilege validation
- Designed for use during PostgreSQL backend initialization when applying stored configuration settings
- Must be called multiple times with different parameter combinations to apply all relevant settings (database-specific, role-specific, and global)
- The snapshot parameter ensures that settings are applied based on a consistent view of the catalog
- Critical component of PostgreSQL's per-database and per-role configuration parameter system

## Simplified Source

```c
// Simplified version of ApplySetting
void ApplySetting(Snapshot snapshot, Oid databaseid, Oid roleid,
                  Relation relsetting, GucSource source) {
    ScanKeyData keys[2];
    SysScanDesc scan;
    HeapTuple tup;

    // Set up scan keys for exact database/role match
    ScanKeyInit(&keys[0], Anum_pg_db_role_setting_setdatabase,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(databaseid));
    ScanKeyInit(&keys[1], Anum_pg_db_role_setting_setrole,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

    // Scan pg_db_role_setting for matching entries
    scan = systable_beginscan(relsetting, DbRoleSettingDatidRolidIndexId,
                              true, snapshot, 2, keys);

    // Process each matching setting tuple
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        bool isnull;
        Datum datum = heap_getattr(tup, Anum_pg_db_role_setting_setconfig,
                                   RelationGetDescr(relsetting), &isnull);

        if (!isnull) {
            ArrayType *config_array = DatumGetArrayTypeP(datum);

            // Apply all settings at SUSET privilege level
            ProcessGUCArray(config_array, PGC_SUSET, source, GUC_ACTION_SET);
        }
    }

    systable_endscan(scan);
}
```

Key simplifications made:
- Focused on the core catalog scanning and setting application logic
- Simplified the key setup and scan process
- Added clear comments for each major step
- Emphasized the SUSET privilege level processing
- Consolidated the tuple processing loop