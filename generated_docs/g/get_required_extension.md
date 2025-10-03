# get_required_extension

## Location
[src/backend/commands/extension.c:1697-1767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1697-L1767)

## Overview
get_required_extension retrieves the OID of a required extension, optionally installing it automatically if CASCADE mode is enabled and the extension is not yet installed.

## Definition

```c
static Oid
get_required_extension(char *reqExtensionName,
					   char *extensionName,
					   char *origSchemaName,
					   bool cascade,
					   List *parents,
					   bool is_create)
```
## Detailed Description
This function handles the resolution of extension dependencies during extension installation. It first attempts to find the required extension by name using get_extension_oid(). If the extension doesn't exist and CASCADE mode is enabled, it automatically installs the required extension by calling CreateExtensionInternal() recursively. The function implements important safety measures including cyclic dependency detection by checking the parents list, and provides helpful error messages with hints when required extensions are missing. It propagates the SCHEMA and CASCADE options to dependent extensions while maintaining proper parent tracking for cycle detection.

## Parameters / Member Variables
- `*reqExtensionName`: Name of the required extension to find or install
- `*extensionName`: Name of the extension that requires this dependency (for error reporting)
- `*origSchemaName`: Original schema name to propagate to dependent extensions
- `cascade`: Whether to automatically install missing required extensions
- `*parents`: List of extension names in current installation chain (for cycle detection)
- `is_create`: Flag indicating if this is a CREATE operation (affects error hint messages)
## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_oid](get_extension_oid.md)
  - [check_valid_extension_name](../c/check_valid_extension_name.md)
  - [list_copy](../l/list_copy.md)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md)

## Notes and Other Information
- This is a static function internal to extension.c that plays a crucial role in dependency management
- Implements cyclic dependency detection by scanning the parents list for the required extension name
- Provides user-friendly NOTICE messages when automatically installing required extensions
- Returns proper error codes (ERRCODE_INVALID_RECURSION, ERRCODE_UNDEFINED_OBJECT) with helpful hints
- Only propagates SCHEMA and CASCADE options to dependent extensions, not other CREATE EXTENSION options
- The parents list is extended with the current extension name before recursive calls to track the installation chain

## Simplified Source

```c
static Oid get_required_extension(char *reqExtensionName, char *extensionName,
                                 char *origSchemaName, bool cascade,
                                 List *parents, bool is_create) {
    Oid reqExtensionOid;

    // Try to find the required extension
    reqExtensionOid = get_extension_oid(reqExtensionName, true);
    if (!OidIsValid(reqExtensionOid)) {
        if (cascade) {
            // Install the required extension automatically
            check_valid_extension_name(reqExtensionName);

            // Check for cyclic dependencies
            foreach(lc, parents) {
                char *pname = (char *) lfirst(lc);
                if (strcmp(pname, reqExtensionName) == 0) {
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_RECURSION),
                             errmsg("cyclic dependency detected between extensions \"%s\" and \"%s\"",
                                    reqExtensionName, extensionName)));
                }
            }

            ereport(NOTICE,
                    (errmsg("installing required extension \"%s\"", reqExtensionName)));

            // Add current extension to parent chain for cycle detection
            List *cascade_parents = lappend(list_copy(parents), extensionName);

            // Recursively create the required extension
            ObjectAddress addr = CreateExtensionInternal(reqExtensionName,
                                                        origSchemaName,
                                                        NULL,  // No specific version
                                                        cascade,
                                                        cascade_parents,
                                                        is_create);

            reqExtensionOid = addr.objectId;
        } else {
            // Required extension missing and no cascade
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("required extension \"%s\" is not installed", reqExtensionName),
                     is_create ?
                     errhint("Use CREATE EXTENSION ... CASCADE to install required extensions too.") : 0));
        }
    }

    return reqExtensionOid;
}
```