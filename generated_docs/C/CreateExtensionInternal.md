# CreateExtensionInternal

## Location
[src/backend/commands/extension.c:1458-1696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1458-L1696)

## Overview
CreateExtensionInternal is the core worker function for the CREATE EXTENSION command that handles the complete installation of a PostgreSQL extension, including dependency resolution, schema management, and script execution.

## Definition

```c
struct stat fst;
```
## Detailed Description
This function performs the complete extension installation process with sophisticated dependency handling. When CASCADE is specified, it recursively installs required extensions while maintaining a "parents" list to detect and prevent cyclic dependencies. The function reads extension control files, determines the optimal installation path (including handling version upgrades through update scripts), manages schema creation and selection, processes prerequisite extensions, inserts the extension record into pg_extension catalog, applies comments, executes installation scripts, and handles any necessary version updates.

The function is designed to handle complex scenarios including:
- Extensions that require specific schemas vs. relocatable extensions
- Version resolution when no direct installation script exists (using update path finding)
- Automatic schema creation for non-relocatable extensions
- Dependency management with cycle detection
- Multi-step installation via update scripts

## Parameters / Member Variables
- : Name of the extension to install
- : Target schema name (can be NULL for relocatable extensions)
- : Specific version to install (NULL uses default version)
- : Whether to automatically install required extensions
- : List of extension names currently being installed (for cycle detection)
- : Flag indicating if this is a CREATE (vs ALTER) operation

## Dependencies
- Functions called/Symbols referenced:
  - [read_extension_control_file](../r/read_extension_control_file.md)
  - [check_valid_version_name](../c/check_valid_version_name.md)
  - [get_extension_script_filename](../g/get_extension_script_filename.md)
  - [get_ext_ver_list](../g/get_ext_ver_list.md)
  - [find_install_path](../f/find_install_path.md)
  - [read_extension_aux_control_file](../r/read_extension_aux_control_file.md)
  - [get_namespace_oid](../g/get_namespace_oid.md)
  - [CreateSchemaCommand](CreateSchemaCommand.md)
  - [get_required_extension](../g/get_required_extension.md)
  - [InsertExtensionTuple](../I/InsertExtensionTuple.md)
  - [CreateComments](CreateComments.md)
  - [execute_extension_script](../e/execute_extension_script.md)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md)
- Called from (representative examples):
  - [get_required_extension](../g/get_required_extension.md)
  - [CreateExtension](CreateExtension.md)

## Notes and Other Information
- This is a static function internal to extension.c, serving as the main worker for both direct CREATE EXTENSION commands and recursive dependency installation
- Handles complex version resolution by finding optimal update paths when direct installation scripts don't exist
- Maintains transaction-level flags (XACT_FLAGS_ACCESSEDTEMPNAMESPACE) when temporary namespaces are accessed
- Uses sophisticated error handling with proper error codes for various failure scenarios
- The parents list parameter is crucial for preventing infinite recursion in cyclic extension dependencies
- Supports both relocatable and non-relocatable extensions with different schema handling strategies

## Simplified Source
```c
static ObjectAddress
CreateExtensionInternal(char *extensionName, char *schemaName,
                        const char *versionName, bool cascade,
                        List *parents, bool is_create)
{
    ExtensionControlFile *pcontrol, *control;
    char *filename;
    List *updateVersions;
    List *requiredExtensions, *requiredSchemas;
    Oid schemaOid = InvalidOid;
    Oid extensionOid;
    ObjectAddress address;

    // Read extension control file
    pcontrol = read_extension_control_file(extensionName);

    // Determine version to install
    if (versionName == NULL) {
        if (pcontrol->default_version)
            versionName = pcontrol->default_version;
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("version to install must be specified")));
    }
    check_valid_version_name(versionName);

    // Find installation script or update path
    filename = get_extension_script_filename(pcontrol, NULL, versionName);
    if (stat(filename, &fst) == 0) {
        updateVersions = NIL;  // Direct installation script exists
    } else {
        // Find update path to target version
        List *evi_list = get_ext_ver_list(pcontrol);
        ExtensionVersionInfo *evi_target = get_ext_ver_info(versionName, &evi_list);
        ExtensionVersionInfo *evi_start = find_install_path(evi_list, evi_target, &updateVersions);

        if (evi_start == NULL)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("extension \"%s\" has no installation script nor update path for version \"%s\"",
                                 pcontrol->name, versionName)));
        versionName = evi_start->name;
    }

    // Read version-specific control parameters
    control = read_extension_aux_control_file(pcontrol, versionName);

    // Determine target schema
    if (schemaName) {
        schemaOid = get_namespace_oid(schemaName, false);
    }

    if (control->schema != NULL) {
        // Extension requires specific schema
        if (schemaName && strcmp(control->schema, schemaName) != 0 && !cascade)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("extension \"%s\" must be installed in schema \"%s\"",
                                 control->name, control->schema)));

        schemaName = control->schema;
        schemaOid = get_namespace_oid(schemaName, true);

        // Create schema if it doesn't exist
        if (!OidIsValid(schemaOid)) {
            CreateSchemaStmt *csstmt = makeNode(CreateSchemaStmt);
            csstmt->schemaname = schemaName;
            csstmt->authrole = NULL;
            csstmt->schemaElts = NIL;
            csstmt->if_not_exists = false;
            CreateSchemaCommand(csstmt, "(generated CREATE SCHEMA command)", -1, -1);
            schemaOid = get_namespace_oid(schemaName, false);
        }
    } else if (!OidIsValid(schemaOid)) {
        // Use default creation namespace
        List *search_path = fetch_search_path(false);
        if (search_path == NIL)
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                           errmsg("no schema has been selected to create in")));
        schemaOid = linitial_oid(search_path);
        schemaName = get_namespace_name(schemaOid);
        list_free(search_path);
    }

    // Handle temp namespace flag
    if (isTempNamespace(schemaOid))
        MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;

    // Process required extensions
    requiredExtensions = NIL;
    requiredSchemas = NIL;
    foreach(lc, control->requires) {
        char *curreq = (char *) lfirst(lc);
        Oid reqext = get_required_extension(curreq, extensionName, origSchemaName,
                                           cascade, parents, is_create);
        Oid reqschema = get_extension_schema(reqext);
        requiredExtensions = lappend_oid(requiredExtensions, reqext);
        requiredSchemas = lappend_oid(requiredSchemas, reqschema);
    }

    // Create extension tuple and dependencies
    address = InsertExtensionTuple(control->name, extowner, schemaOid,
                                  control->relocatable, versionName,
                                  PointerGetDatum(NULL), PointerGetDatum(NULL),
                                  requiredExtensions);
    extensionOid = address.objectId;

    // Apply comment if specified
    if (control->comment != NULL)
        CreateComments(extensionOid, ExtensionRelationId, 0, control->comment);

    // Execute installation script
    execute_extension_script(extensionOid, control, NULL, versionName,
                            requiredSchemas, schemaName, schemaOid);

    // Apply any additional updates
    ApplyExtensionUpdates(extensionOid, pcontrol, versionName, updateVersions,
                         origSchemaName, cascade, is_create);

    return address;
}
```