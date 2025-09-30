# AlterExtensionNamespace

## Location
[src/backend/commands/extension.c:2772-2986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L2772-L2986)

## Overview
Executes ALTER EXTENSION SET SCHEMA command to move an extension and all its member objects from one schema to another schema.

## Definition

```c
ObjectAddress
AlterExtensionNamespace(const char *extensionName, const char *newschema, Oid *oldschema)
```
## Detailed Description
This function implements the ALTER EXTENSION SET SCHEMA command, which relocates an extension and all its dependent objects to a new schema. The operation requires the extension to be marked as relocatable in its control file. The function performs extensive validation including ownership checks, permission checks, dependency loop detection, and no-relocate constraint enforcement.

The function iterates through all objects that depend on the extension (via pg_depend) and calls AlterObjectNamespace_oid for each one to move them to the new schema. It ensures all objects are consistently moved and maintains dependency relationships. The function also handles special cases like preventing moves that would create dependency loops and respecting no-relocate requests from dependent extensions.

## Parameters / Member Variables
-  (const char *): Name of the extension to relocate
-  (const char *): Name of the target schema
-  (Oid *): Optional output parameter to receive the OID of the old schema

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_oid](../g/get_extension_oid.md): Resolves extension name to OID
  - [LookupCreationNamespace](../L/LookupCreationNamespace.md): Resolves target schema name to OID
  - [object_ownercheck](../o/object_ownercheck.md): Verifies ownership of extension
  - [object_aclcheck](../o/object_aclcheck.md): Checks creation permissions in target schema
  - [getExtensionOfObject](../g/getExtensionOfObject.md): Checks for dependency loops
  - [table_open](../t/table_open.md)/systable_beginscan: Accesses pg_extension and pg_depend catalogs
  - [read_extension_control_file](../r/read_extension_control_file.md): Reads extension control file for no_relocate list
  - [AlterObjectNamespace_oid](AlterObjectNamespace_oid.md): Moves individual objects to new schema
  - [changeDependencyFor](../c/changeDependencyFor.md): Updates schema dependency for extension
  - InvokeObjectPostAlterHook: Triggers post-alter hooks
- Called from (representative examples):
  - [ExecAlterObjectSchemaStmt](../E/ExecAlterObjectSchemaStmt.md): Main entry point for ALTER ... SET SCHEMA commands

## Notes and Other Information
- Requires extension to be marked as relocatable in its control file
- Performs comprehensive permission checks (ownership and CREATE rights in target schema)
- Prevents dependency loops by checking if target schema is owned by the extension
- Respects no-relocate constraints from dependent extensions
- Ensures all extension objects are moved consistently to the same schema
- Returns InvalidObjectAddress if extension is already in target schema
- Updates both pg_extension.extnamespace and dependency records
- Uses RowExclusiveLock on pg_extension to prevent concurrent modifications
- Located in src/backend/commands/extension.c:2772-2986

## Simplified Source

```c
ObjectAddress
AlterExtensionNamespace(const char *extensionName, const char *newschema, Oid *oldschema)
{
    // Get extension and target namespace OIDs
    Oid extensionOid = get_extension_oid(extensionName, false);
    Oid nspOid = LookupCreationNamespace(newschema);

    // Permission checks: must own extension and have CREATE rights in target schema
    if (!object_ownercheck(ExtensionRelationId, extensionOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_EXTENSION, extensionName);

    AclResult aclresult = object_aclcheck(NamespaceRelationId, nspOid, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, OBJECT_SCHEMA, newschema);

    // Prevent dependency loops: target schema cannot be owned by extension
    if (getExtensionOfObject(NamespaceRelationId, nspOid) == extensionOid)
        ereport(ERROR, "cannot move extension into schema it contains");

    // Get extension tuple from catalog
    Relation extRel = table_open(ExtensionRelationId, RowExclusiveLock);
    HeapTuple extTup = /* find extension tuple by OID */;
    Form_pg_extension extForm = (Form_pg_extension) GETSTRUCT(extTup);

    // If already in target schema, do nothing
    if (extForm->extnamespace == nspOid) {
        table_close(extRel, RowExclusiveLock);
        return InvalidObjectAddress;
    }

    // Check extension is relocatable
    if (!extForm->extrelocatable)
        ereport(ERROR, "extension does not support SET SCHEMA");

    Oid oldNspOid = extForm->extnamespace;

    // Move all extension member objects to new schema
    Relation depRel = table_open(DependRelationId, AccessShareLock);
    SysScanDesc depScan = /* scan dependencies on this extension */;

    while (HeapTupleIsValid(depTup = systable_getnext(depScan))) {
        Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);

        // Check for no-relocate constraints from dependent extensions
        if (pg_depend->deptype == DEPENDENCY_NORMAL &&
            pg_depend->classid == ExtensionRelationId) {
            // Check if dependent extension has no_relocate request
            ExtensionControlFile *dcontrol = read_extension_control_file(depextname);
            // Error if this extension is in no_relocate list
        }

        // Skip non-membership dependencies
        if (pg_depend->deptype != DEPENDENCY_EXTENSION)
            continue;

        // Move the dependent object to new schema
        ObjectAddress dep = {pg_depend->classid, pg_depend->objid, pg_depend->objsubid};
        AlterObjectNamespace_oid(dep.classId, dep.objectId, nspOid, objsMoved);
    }

    // Update extension's schema in catalog
    extForm->extnamespace = nspOid;
    CatalogTupleUpdate(extRel, &extTup->t_self, extTup);

    // Update dependency record for extension's schema
    changeDependencyFor(ExtensionRelationId, extensionOid,
                       NamespaceRelationId, oldNspOid, nspOid);

    // Report old schema if requested
    if (oldschema)
        *oldschema = oldNspOid;

    // Cleanup and return
    table_close(extRel, RowExclusiveLock);
    InvokeObjectPostAlterHook(ExtensionRelationId, extensionOid, 0);

    ObjectAddress extAddr;
    ObjectAddressSet(extAddr, ExtensionRelationId, extensionOid);
    return extAddr;
}
```