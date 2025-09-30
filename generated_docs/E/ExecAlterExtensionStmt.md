# ExecAlterExtensionStmt

## Location
[src/backend/commands/extension.c:2987-3133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2987-L3133)

## Overview
Executes ALTER EXTENSION UPDATE command to update an extension from its current version to a specified target version by running the appropriate sequence of update scripts.

## Definition

```c
ObjectAddress
ExecAlterExtensionStmt(ParseState *pstate, AlterExtensionStmt *stmt)
```
## Detailed Description
This function implements the ALTER EXTENSION UPDATE command, which upgrades or downgrades an extension to a different version. The function validates the extension exists, determines the current version, identifies the target version (from statement options or extension default), and calculates the sequence of update scripts needed to reach the target version.

The function prevents nested ALTER EXTENSION operations using the global creating_extension flag. It performs ownership checks, reads the extension control file to understand available versions and update paths, and delegates the actual update execution to ApplyExtensionUpdates. If the extension is already at the target version, it reports a notice and returns without action.

## Parameters / Member Variables
-  (ParseState *): Parse state for the SQL statement (used for error reporting)
-  (AlterExtensionStmt *): Parsed ALTER EXTENSION statement containing extension name and options

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)/systable_beginscan: Accesses pg_extension catalog
  - [heap_getattr](../h/heap_getattr.md): Retrieves current extension version
  - [text_to_cstring](../t/text_to_cstring.md): Converts version datum to string
  - [object_ownercheck](../o/object_ownercheck.md): Verifies ownership of extension
  - [read_extension_control_file](../r/read_extension_control_file.md): Reads extension control file
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md): Reports conflicting statement options
  - [check_valid_version_name](../c/check_valid_version_name.md): Validates version name format
  - [identify_update_path](../i/identify_update_path.md): Determines sequence of update scripts
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md): Executes the actual update process
  - ObjectAddressSet: Creates return address object
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command dispatcher for ALTER EXTENSION

## Notes and Other Information
- Uses global creating_extension flag to prevent nested extension operations
- Supports specifying target version via new_version option or uses control file default
- Reports notice and exits early if extension is already at target version
- Performs comprehensive ownership and version validation
- The actual update execution is delegated to ApplyExtensionUpdates function
- Handles both upgrades and downgrades by finding appropriate update path
- Returns InvalidObjectAddress if no update is needed (already at target version)
- Uses AccessShareLock for reading pg_extension catalog
- Located in src/backend/commands/extension.c:2987-3133

## Simplified Source

```c
ObjectAddress ExecAlterExtensionStmt(ParseState *pstate, AlterExtensionStmt *stmt)
{
    char *versionName;
    char *oldVersionName;
    ExtensionControlFile *control;
    Oid extensionOid;
    List *updateVersions;
    ObjectAddress address;

    // Prevent nested extension operations
    if (creating_extension)
        ereport(ERROR, (errmsg("nested ALTER EXTENSION is not supported")));

    // Look up extension in pg_extension catalog
    Relation extRel = table_open(ExtensionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_extension_extname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(stmt->extname));

    SysScanDesc extScan = systable_beginscan(extRel, ExtensionNameIndexId,
                                             true, NULL, 1, key);
    HeapTuple extTup = systable_getnext(extScan);

    if (!HeapTupleIsValid(extTup))
        ereport(ERROR, (errmsg("extension \"%s\" does not exist", stmt->extname)));

    extensionOid = ((Form_pg_extension) GETSTRUCT(extTup))->oid;

    // Get current version
    Datum datum = heap_getattr(extTup, Anum_pg_extension_extversion,
                              RelationGetDescr(extRel), &isnull);
    oldVersionName = text_to_cstring(DatumGetTextPP(datum));

    systable_endscan(extScan);
    table_close(extRel, AccessShareLock);

    // Check ownership
    if (!object_ownercheck(ExtensionRelationId, extensionOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EXTENSION, stmt->extname);

    // Read control file and determine target version
    control = read_extension_control_file(stmt->extname);

    // Parse options for new_version
    foreach(lc, stmt->options)
    {
        DefElem *defel = (DefElem *) lfirst(lc);
        if (strcmp(defel->defname, "new_version") == 0)
            versionName = strVal(defel->arg);
    }

    if (!versionName)
        versionName = control->default_version;

    check_valid_version_name(versionName);

    // Check if already at target version
    if (strcmp(oldVersionName, versionName) == 0)
    {
        ereport(NOTICE, (errmsg("version \"%s\" of extension \"%s\" is already installed",
                               versionName, stmt->extname)));
        return InvalidObjectAddress;
    }

    // Find update path and apply updates
    updateVersions = identify_update_path(control, oldVersionName, versionName);
    ApplyExtensionUpdates(extensionOid, control, oldVersionName, updateVersions,
                          NULL, false, false);

    ObjectAddressSet(address, ExtensionRelationId, extensionOid);
    return address;
}
```