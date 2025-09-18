# get_extension_control_directory

## Location
src/backend/commands/extension.c: 376 - 388

## Overview
Returns the absolute path to the directory where PostgreSQL extension control files are stored, typically $SHAREPATH/extension.

## Definition
static char *get_extension_control_directory(void)

## Detailed Description
This static function constructs and returns the full filesystem path to the directory containing extension control files. Extension control files (.control files) contain metadata about extensions including default versions, dependencies, comments, and configuration parameters. The function uses PostgreSQL's standard path resolution mechanisms to locate the share directory and appends the "extension" subdirectory to create the complete path.

The function allocates memory for the result string using PostgreSQL's memory management system (palloc) and formats the path using snprintf to ensure buffer safety. This path is used throughout the extension management system when searching for or accessing extension control files.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - get_share_path (PostgreSQL path utility function)
  - palloc (PostgreSQL memory allocation function)
  - snprintf (standard C library function)
- Called from (representative examples):
  - get_extension_script_directory
  - pg_available_extensions
  - pg_available_extension_versions
  - extension_file_exists

## Notes and Other Information
- This is a static function, meaning it's only accessible within the src/backend/commands/extension.c file
- The function allocates memory using palloc, which is automatically freed when the current memory context is destroyed
- Uses MAXPGPATH constant to ensure the path buffer is sufficiently large for PostgreSQL path names
- The returned path typically resolves to something like /usr/share/postgresql/extension or similar depending on the installation
- my_exec_path is a global variable containing the path to the PostgreSQL executable, used as a reference point for finding related directories