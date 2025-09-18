# get_extension_control_filename

## Location
src/backend/commands/extension.c: 389 - 402

## Overview
Constructs and returns the complete filesystem path to a specific extension's control file given the extension name.

## Definition
static char *get_extension_control_filename(const char *extname)

## Detailed Description
This static function builds the full path to a specific extension's control file by combining the PostgreSQL share directory path with the extension subdirectory and the extension name. The resulting path follows the format $SHAREPATH/extension/extensionname.control. Extension control files contain essential metadata about extensions including default versions, dependencies, module specifications, and other configuration parameters required for extension management.

The function uses PostgreSQL's standard path resolution mechanisms and memory management to construct a properly formatted path string. This function is fundamental to the extension loading and management process, as control files must be located and parsed before extensions can be created or managed.

## Parameters / Member Variables
- `extname`: The name of the extension for which to construct the control file path

## Dependencies
- Functions called/Symbols referenced:
  - get_share_path (PostgreSQL path utility function)
  - palloc (PostgreSQL memory allocation function)
  - snprintf (standard C library function)
- Called from (representative examples):
  - parse_extension_control_file

## Notes and Other Information
- This is a static function, meaning it's only accessible within the src/backend/commands/extension.c file
- The function allocates memory using palloc, which is automatically freed when the current memory context is destroyed
- Uses MAXPGPATH constant to ensure the path buffer is sufficiently large for PostgreSQL path names
- The resulting path typically looks like /usr/share/postgresql/extension/extensionname.control
- No validation is performed on the extension name parameter - the caller is responsible for ensuring it's valid
- my_exec_path is a global variable containing the path to the PostgreSQL executable used as a reference for path resolution
- The .control extension is automatically appended to the provided extension name