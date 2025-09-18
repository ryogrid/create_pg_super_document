# ExtensionControlFile

## Location
src/backend/commands/extension.c: 77 - 93

## Overview
ExtensionControlFile is an internal data structure that holds the parsed contents of a PostgreSQL extension control file, containing metadata and configuration information necessary for extension management operations.

## Definition
```c
typedef struct ExtensionControlFile
{
    char       *name;              /* name of the extension */
    char       *directory;         /* directory for script files */
    char       *default_version;   /* default install target version, if any */
    char       *module_pathname;   /* string to substitute for MODULE_PATHNAME */
    char       *comment;           /* comment, if any */
    char       *schema;            /* target schema (allowed if !relocatable) */
    bool        relocatable;       /* is ALTER EXTENSION SET SCHEMA supported? */
    bool        superuser;         /* must be superuser to install? */
    bool        trusted;           /* allow becoming superuser on the fly? */
    int         encoding;          /* encoding of the script file, or -1 */
    List       *requires;          /* names of prerequisite extensions */
    List       *no_relocate;       /* names of prerequisite extensions that
                                    * should not be relocated */
} ExtensionControlFile;
```

## Detailed Description
The ExtensionControlFile structure serves as the in-memory representation of a parsed extension control file (.control). Control files contain metadata about PostgreSQL extensions including installation requirements, security settings, dependencies, and deployment configuration. This structure is populated by parsing functions and used throughout the extension management system to make decisions about extension installation, updates, and schema management. The structure centralizes all control file information needed for safe and proper extension handling.

## Parameters / Member Variables
- `name`: The extension name as specified in the control file
- `directory`: Directory path where the extension script files are located
- `default_version`: The default version to install if no specific version is requested
- `module_pathname`: Template string that gets substituted with the actual module path during installation
- `comment`: Human-readable description of the extension
- `schema`: Target schema name for non-relocatable extensions
- `relocatable`: Boolean flag indicating whether the extension supports ALTER EXTENSION SET SCHEMA
- `superuser`: Boolean flag indicating whether superuser privileges are required for installation
- `trusted`: Boolean flag indicating whether the extension can safely elevate to superuser privileges
- `encoding`: Character encoding of the script files (-1 if not specified)
- `requires`: List of prerequisite extension names that must be installed first
- `no_relocate`: List of prerequisite extensions that should not be relocated when this extension is moved

## Dependencies
- Functions called/Symbols referenced:
  - comment (member field)
  - superuser (member field)
- Called from (representative examples):
  - read_extension_control_file
  - parse_extension_control_file
  - CreateExtensionInternal
  - get_ext_ver_list
  - identify_update_path
  - extension_is_trusted
  - execute_extension_script

## Notes and Other Information
This structure is central to PostgreSQL extension management and is used extensively throughout src/backend/commands/extension.c. The control file format and this structure define the interface between extension authors and the PostgreSQL extension system. Security-related fields (superuser, trusted) are particularly important for maintaining database security during extension operations.