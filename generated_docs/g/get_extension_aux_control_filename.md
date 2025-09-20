# get_extension_aux_control_filename

## Location
[src/backend/commands/extension.c:426-443](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L426-L443)

## Overview
Constructs the filename for an auxiliary extension control file that contains version-specific metadata for a PostgreSQL extension.

## Definition

```c
static char *
get_extension_aux_control_filename(ExtensionControlFile *control,
								   const char *version)
```
## Detailed Description
This function generates the complete file path for an auxiliary control file associated with a specific version of a PostgreSQL extension. Auxiliary control files follow the naming pattern "extension_name--version.control" and are stored in the extension's script directory. These files contain version-specific control information that supplements the main extension control file.

The function constructs the path by combining the extension's script directory with a filename formatted as "name--version.control", where 'name' comes from the control structure and 'version' is the specified version string.

## Parameters / Member Variables
- : Pointer to ExtensionControlFile structure containing extension metadata including the extension name
- : String specifying the version for which to construct the auxiliary control filename

## Dependencies
- Functions called/Symbols referenced:
  - [get_extension_script_directory](get_extension_script_directory.md)
  - [palloc](../p/palloc.md)
  - snprintf
  - [pfree](../p/pfree.md)
- Types referenced:
  - [ExtensionControlFile](../E/ExtensionControlFile.md)
- Called from (representative examples):
  - [parse_extension_control_file](../p/parse_extension_control_file.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c source file
- The function allocates memory using palloc() which must be freed by the caller
- Uses MAXPGPATH constant to limit the maximum path length
- The auxiliary control file naming convention uses "--" as a separator between extension name and version
- Properly manages memory by freeing the temporary scriptdir allocation before returning