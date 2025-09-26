# read_extension_control_file

## Location
[src/backend/commands/extension.c:648-676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L648-L676)

## Overview
Reads and parses the primary control file for a specified extension, returning a fully initialized ExtensionControlFile structure with default values and parsed configuration.

## Definition
```c
static ExtensionControlFile *read_extension_control_file(const char *extname)
```

## Detailed Description
This function serves as the main entry point for reading an extension's primary control file. It creates a new ExtensionControlFile structure, initializes it with PostgreSQL's default values for extension parameters, and then parses the extension's control file to override these defaults with extension-specific configuration.

The function establishes sensible defaults before parsing: extensions are not relocatable by default, require superuser privileges, are not trusted, and have no specific encoding requirement. These defaults ensure that extensions work securely even if the control file is minimal or missing certain parameters.

After setting defaults, it calls parse_extension_control_file() with version=NULL to parse the primary control file and populate the structure with the extension's actual configuration.

## Parameters / Member Variables
- `extname`: Name of the extension for which to read the control file

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [pstrdup](../p/pstrdup.md)
  - [parse_extension_control_file](../p/parse_extension_control_file.md)
- Types referenced:
  - [ExtensionControlFile](../E/ExtensionControlFile.md)
- Called from (representative examples):
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md)
  - [pg_available_extensions](../p/pg_available_extensions.md)
  - [pg_available_extension_versions](../p/pg_available_extension_versions.md)
  - [pg_extension_update_paths](../p/pg_extension_update_paths.md)
  - [AlterExtensionNamespace](../A/AlterExtensionNamespace.md)
  - [ExecAlterExtensionStmt](../E/ExecAlterExtensionStmt.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the extension.c source file
- Uses palloc0() to ensure all pointer fields start as NULL and all boolean/numeric fields start as 0
- Sets conservative security defaults: relocatable=false, superuser=true, trusted=false
- The encoding field defaults to -1, indicating no specific encoding requirement
- Returns a newly allocated ExtensionControlFile structure that must be freed by the caller
- Only reads the primary control file - auxiliary control files are handled separately
- The function does not validate extension existence; errors are handled by the underlying parse function