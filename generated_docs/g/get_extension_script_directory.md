# get_extension_script_directory

## Location
src/backend/commands/extension.c: 403 - 425

## Overview
Determines and returns the directory path where extension script files are located, based on the directory specification in the extension's control file.

## Definition
static char *get_extension_script_directory(ExtensionControlFile *control)

## Detailed Description
This static function resolves the directory path for extension script files by examining the directory parameter in the extension's control file structure. The function handles three different scenarios for directory specification:

1. **No directory specified**: If the control file doesn't specify a directory, it defaults to the standard extension control directory ($SHAREPATH/extension)
2. **Absolute path**: If the directory is specified as an absolute path, it uses that path directly
3. **Relative path**: If the directory is specified as a relative path, it resolves it relative to PostgreSQL's share directory

This flexible approach allows extensions to store their script files in custom locations while maintaining backward compatibility with the standard directory structure. Script files typically include installation scripts, upgrade scripts, and other SQL files needed for extension management.

## Parameters / Member Variables
- `control`: Pointer to an ExtensionControlFile structure containing parsed control file information, including the optional directory parameter

## Dependencies
- Functions called/Symbols referenced:
  - [ExtensionControlFile](../E/ExtensionControlFile.md) (structure type)
  - [get_extension_control_directory](get_extension_control_directory.md) (returns default extension directory)
  - is_absolute_path (checks if path is absolute)
  - [get_share_path](get_share_path.md) (PostgreSQL path utility function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL string duplication function)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - snprintf (standard C library function)
- Called from (representative examples):
  - [get_extension_aux_control_filename](get_extension_aux_control_filename.md)
  - [get_extension_script_filename](get_extension_script_filename.md)
  - [get_ext_ver_list](get_ext_ver_list.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the src/backend/commands/extension.c file
- The function allocates memory using palloc or pstrdup, which is automatically freed when the current memory context is destroyed
- Uses MAXPGPATH constant to ensure the path buffer is sufficiently large for PostgreSQL path names
- The directory parameter in the control file is optional - when omitted, extensions use the standard extension directory
- Supports both absolute and relative paths, providing flexibility in extension deployment
- my_exec_path is a global variable containing the path to the PostgreSQL executable used as a reference for path resolution
- The control parameter should not be NULL, as the function accesses its directory member without validation