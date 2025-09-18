# CreateExtensionInternal

## Location
[src/backend/commands/extension.c:1458-1696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L1458-L1696)

## Overview
CreateExtensionInternal is the core worker function for the CREATE EXTENSION command that handles the complete installation of a PostgreSQL extension, including dependency resolution, schema management, and script execution.

## Definition


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