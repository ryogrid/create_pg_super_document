# get_extension_script_filename

## Location
[src/backend/commands/extension.c:444-475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L444-L475)

## Overview
Constructs the filename for an extension SQL script file, supporting both installation scripts and upgrade scripts between versions.

## Definition
```c
static char *get_extension_script_filename(ExtensionControlFile *control, const char *from_version, const char *version)
```

## Detailed Description
This function generates the complete file path for an extension SQL script file. It handles two types of script files:
1. Installation scripts: When from_version is NULL, it creates a filename in the format "extension_name--version.sql" for installing a specific version
2. Update scripts: When from_version is provided, it creates a filename in the format "extension_name--from_version--version.sql" for upgrading from one version to another

The function constructs the path by combining the extension's script directory with the appropriately formatted filename based on whether an upgrade path is being specified.

## Parameters / Member Variables
- `control`: Pointer to ExtensionControlFile structure containing extension metadata including the extension name
- `from_version`: Source version string for upgrade scripts, or NULL for installation scripts
- `version`: Target version string for the script

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_script_directory](get_extension_script_directory.md)
  - [palloc](../p/palloc.md)
  - snprintf
  - [pfree](../p/pfree.md)
- Types referenced:
  - [ExtensionControlFile](../E/ExtensionControlFile.md)
- Called from (representative examples):
  - [execute_extension_script](../e/execute_extension_script.md)
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c source file
- The function allocates memory using palloc() which must be freed by the caller
- Uses MAXPGPATH constant to limit the maximum path length
- The script file naming convention uses "--" as separators between extension name and version identifiers
- Supports both installation (single version) and update (from-to version) script naming patterns
- Properly manages memory by freeing the temporary scriptdir allocation before returning