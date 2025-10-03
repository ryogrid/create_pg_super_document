# ApplyExtensionUpdates

## Location
[src/backend/commands/extension.c:3134-3291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L3134-L3291)

## Overview
Applies a series of update scripts sequentially to upgrade an extension through multiple versions, treating each step as an individual ALTER EXTENSION UPDATE command.

## Definition

```c
static void
ApplyExtensionUpdates(Oid extensionOid,
					  ExtensionControlFile *pcontrol,
					  const char *initialVersion,
					  List *updateVersions,
					  char *origSchemaName,
					  bool cascade,
					  bool is_create)
```
## Detailed Description
This function manages the complex process of updating PostgreSQL extensions through multiple version increments. It iterates through a list of target versions, applying each update script in sequence while maintaining proper metadata and dependency tracking. For each version update, it:

1. Loads the version-specific control file parameters
2. Updates the pg_extension catalog entry with new version and relocatability information
3. Resolves and installs prerequisite extensions if needed
4. Updates dependency records to reflect current requirements
5. Executes the actual update script for the version transition
6. Triggers post-alter hooks for proper event handling

The function ensures that older update scripts remain functional even when newer versions have different control parameters by treating each step as a discrete update operation.

## Parameters / Member Variables
- `extensionOid`: OID of the extension being updated
- `*pcontrol`: Primary extension control file containing base configuration
- `*initialVersion`: Starting version name for the update sequence
- `*updateVersions`: List of target version names to update through sequentially
- `*origSchemaName`: Original schema name where extension was installed
- `cascade`: Whether to automatically install missing prerequisite extensions
- `is_create`: Flag indicating if this is part of extension creation process
## Dependencies
- Functions called/Symbols referenced:
  - [read_extension_aux_control_file](../r/read_extension_aux_control_file.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_getnext
  - [get_namespace_name](../g/get_namespace_name.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [get_required_extension](../g/get_required_extension.md)
  - [get_extension_schema](../g/get_extension_schema.md)
  - [deleteDependencyRecordsForClass](../d/deleteDependencyRecordsForClass.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - InvokeObjectPostAlterHook
  - [execute_extension_script](../e/execute_extension_script.md)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (during extension creation)
  - [ExecAlterExtensionStmt](../E/ExecAlterExtensionStmt.md) (during explicit extension updates)

## Notes and Other Information
- This function is static and only used internally within extension.c
- Each update step is treated as a separate transaction-level operation with proper catalog updates
- The function handles prerequisite extension resolution and dependency graph updates at each step
- Proper locking (RowExclusiveLock) is maintained on the extension catalog during updates
- The design ensures backward compatibility with older update scripts even when control file parameters change in newer versions

## Simplified Source

```c
static void
ApplyExtensionUpdates(Oid extensionOid,
                      ExtensionControlFile *pcontrol,
                      const char *initialVersion,
                      List *updateVersions,
                      char *origSchemaName,
                      bool cascade,
                      bool is_create)
{
    const char *oldVersionName = initialVersion;
    ListCell *lcv;

    // Apply each version update sequentially
    foreach(lcv, updateVersions)
    {
        char *versionName = (char *) lfirst(lcv);
        ExtensionControlFile *control;
        char *schemaName;
        Oid schemaOid;
        List *requiredExtensions;
        List *requiredSchemas;

        // Load version-specific control file
        control = read_extension_aux_control_file(pcontrol, versionName);

        // Update pg_extension catalog entry
        Relation extRel = table_open(ExtensionRelationId, RowExclusiveLock);

        // Find and update the extension tuple
        ScanKeyData key[1];
        ScanKeyInit(&key[0], Anum_pg_extension_oid, BTEqualStrategyNumber,
                   F_OIDEQ, ObjectIdGetDatum(extensionOid));

        SysScanDesc extScan = systable_beginscan(extRel, ExtensionOidIndexId, true, NULL, 1, key);
        HeapTuple extTup = systable_getnext(extScan);

        Form_pg_extension extForm = (Form_pg_extension) GETSTRUCT(extTup);
        schemaOid = extForm->extnamespace;
        schemaName = get_namespace_name(schemaOid);

        // Update version and relocatable flag
        Datum values[Natts_pg_extension];
        bool nulls[Natts_pg_extension];
        bool repl[Natts_pg_extension];

        memset(values, 0, sizeof(values));
        memset(nulls, 0, sizeof(nulls));
        memset(repl, 0, sizeof(repl));

        values[Anum_pg_extension_extrelocatable - 1] = BoolGetDatum(control->relocatable);
        repl[Anum_pg_extension_extrelocatable - 1] = true;
        values[Anum_pg_extension_extversion - 1] = CStringGetTextDatum(versionName);
        repl[Anum_pg_extension_extversion - 1] = true;

        extTup = heap_modify_tuple(extTup, RelationGetDescr(extRel), values, nulls, repl);
        CatalogTupleUpdate(extRel, &extTup->t_self, extTup);

        systable_endscan(extScan);
        table_close(extRel, RowExclusiveLock);

        // Handle prerequisite extensions
        requiredExtensions = NIL;
        requiredSchemas = NIL;
        ListCell *lc;

        foreach(lc, control->requires)
        {
            char *curreq = (char *) lfirst(lc);
            Oid reqext = get_required_extension(curreq, control->name,
                                              origSchemaName, cascade, NIL, is_create);
            Oid reqschema = get_extension_schema(reqext);

            requiredExtensions = lappend_oid(requiredExtensions, reqext);
            requiredSchemas = lappend_oid(requiredSchemas, reqschema);
        }

        // Update dependencies on prerequisite extensions
        deleteDependencyRecordsForClass(ExtensionRelationId, extensionOid,
                                       ExtensionRelationId, DEPENDENCY_NORMAL);

        ObjectAddress myself = {ExtensionRelationId, extensionOid, 0};
        foreach(lc, requiredExtensions)
        {
            ObjectAddress otherext = {ExtensionRelationId, lfirst_oid(lc), 0};
            recordDependencyOn(&myself, &otherext, DEPENDENCY_NORMAL);
        }

        InvokeObjectPostAlterHook(ExtensionRelationId, extensionOid, 0);

        // Execute the update script
        execute_extension_script(extensionOid, control, oldVersionName, versionName,
                               requiredSchemas, schemaName, schemaOid);

        oldVersionName = versionName;
    }
}
```