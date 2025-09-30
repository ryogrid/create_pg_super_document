# AlterSetting

## Location
[src/backend/catalog/pg_db_role_setting.c:24-169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_db_role_setting.c#L24-L169)

## Overview
AlterSetting is a core PostgreSQL catalog function that manages database and role-specific configuration parameter settings by updating the pg_db_role_setting system catalog table.

## Definition
```c
void AlterSetting(Oid databaseid, Oid roleid, VariableSetStmt *setstmt)
```

## Detailed Description
AlterSetting handles modifications to database and role-specific configuration parameters stored in the pg_db_role_setting catalog. It supports three main operations:

1. **RESET ALL**: Resets all configuration parameters for a specific database/role combination by calling GUCArrayReset. If any parameters remain after reset, the tuple is updated; otherwise, it's deleted.

2. **Update existing settings**: For existing pg_db_role_setting tuples, it either adds new parameters (via GUCArrayAdd) or removes them (via GUCArrayDelete). If the configuration array becomes empty, the tuple is deleted.

3. **Insert new settings**: Creates a new pg_db_role_setting tuple when setting parameters for a database/role combination that doesn't exist yet (only for non-RESET operations).

The function uses a system catalog scan to locate existing settings and performs appropriate catalog operations (insert, update, or delete) based on the operation type and current state.

## Parameters / Member Variables
- `databaseid`: OID of the target database (InvalidOid for global role settings)
- `roleid`: OID of the target role (InvalidOid for database-wide settings)
- `setstmt`: VariableSetStmt containing the parameter name, value, and operation type (SET, RESET, RESET ALL)

## Dependencies
- Functions called/Symbols referenced:
  - [ExtractSetVariableArgs](../E/ExtractSetVariableArgs.md)
  - [table_open](../t/table_open.md)/table_close
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [heap_getattr](../h/heap_getattr.md)
  - [GUCArrayReset](../G/GUCArrayReset.md)/GUCArrayAdd/GUCArrayDelete
  - [heap_modify_tuple](../h/heap_modify_tuple.md)/heap_form_tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)/CatalogTupleInsert/CatalogTupleDelete
  - InvokeObjectPostAlterHookArg
- Called from (representative examples):
  - [AlterDatabaseSet](AlterDatabaseSet.md) (src/backend/commands/dbcommands.c:2612)
  - [AlterRoleSet](AlterRoleSet.md) (src/backend/commands/user.c:1080)

## Notes and Other Information
- Acquires RowExclusiveLock on the pg_db_role_setting relation to ensure consistency during modifications
- Uses the DbRoleSettingDatidRolidIndexId index for efficient tuple lookup based on database and role OIDs
- Triggers post-alter hooks to notify other subsystems of configuration changes
- Handles both database-specific (databaseid != InvalidOid) and role-specific (roleid != InvalidOid) settings
- The function is transaction-safe and maintains catalog consistency through proper locking and tuple management

## Simplified Source

```c
void
AlterSetting(Oid databaseid, Oid roleid, VariableSetStmt *setstmt)
{
    char *valuestr;
    HeapTuple tuple;
    Relation rel;
    ScanKeyData scankey[2];
    SysScanDesc scan;

    // Extract the value from the SET statement
    valuestr = ExtractSetVariableArgs(setstmt);

    // Open pg_db_role_setting catalog and scan for existing tuple
    rel = table_open(DbRoleSettingRelationId, RowExclusiveLock);

    ScanKeyInit(&scankey[0], Anum_pg_db_role_setting_setdatabase,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(databaseid));
    ScanKeyInit(&scankey[1], Anum_pg_db_role_setting_setrole,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roleid));

    scan = systable_beginscan(rel, DbRoleSettingDatidRolidIndexId, true,
                              NULL, 2, scankey);
    tuple = systable_getnext(scan);

    // Handle three cases based on operation type and existing tuple
    if (setstmt->kind == VAR_RESET_ALL) {
        // RESET ALL: reset configuration array
        if (HeapTupleIsValid(tuple)) {
            ArrayType *new_config = reset_all_guc_settings(tuple, rel);
            if (new_config)
                update_catalog_tuple(rel, tuple, new_config);
            else
                CatalogTupleDelete(rel, &tuple->t_self);
        }
    }
    else if (HeapTupleIsValid(tuple)) {
        // Update existing tuple
        ArrayType *current_config = get_current_config_array(tuple, rel);
        ArrayType *new_config;

        if (valuestr)
            new_config = GUCArrayAdd(current_config, setstmt->name, valuestr);
        else
            new_config = GUCArrayDelete(current_config, setstmt->name);

        if (new_config)
            update_catalog_tuple(rel, tuple, new_config);
        else
            CatalogTupleDelete(rel, &tuple->t_self);
    }
    else if (valuestr) {
        // Insert new tuple (only for non-RESET operations)
        ArrayType *new_config = GUCArrayAdd(NULL, setstmt->name, valuestr);
        insert_new_setting_tuple(rel, databaseid, roleid, new_config);
    }

    // Trigger post-alter hook and cleanup
    InvokeObjectPostAlterHookArg(DbRoleSettingRelationId, databaseid, 0, roleid, false);
    systable_endscan(scan);
    table_close(rel, NoLock);
}
```