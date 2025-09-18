# execute_extension_script

## Location
src/backend/commands/extension.c: 870 - 1142

## Overview
Executes the appropriate SQL script file for installing or updating a PostgreSQL extension, handling security, environment setup, variable substitution, and script execution.

## Definition
```c
static void execute_extension_script(Oid extensionOid, ExtensionControlFile *control,
                                    const char *from_version, const char *version,
                                    List *requiredSchemas, const char *schemaName, Oid schemaOid)
```

## Detailed Description
This is the core function responsible for executing extension scripts during PostgreSQL extension installation or updates. It implements a comprehensive security and environment management system that includes:

**Security Management:**
- Enforces superuser requirements or trusted extension policies
- Temporarily switches to bootstrap superuser for trusted extensions
- Validates user privileges using extension_is_trusted()

**Environment Configuration:**
- Sets up search path to include target schema and prerequisite extension schemas
- Configures GUC variables (client_min_messages, log_min_messages, check_function_bodies)
- Manages creating_extension global state for dependency tracking

**Script Processing:**
- Reads and processes the extension script file with encoding conversion
- Performs variable substitution for special tokens:
  - @extowner@ → extension owner username
  - @extschema@ → target schema name (for non-relocatable extensions)
  - @extschema:extension_name@ → required extension schema names
  - MODULE_PATHNAME → module path from control file
- Removes \echo commands to prevent psql-specific output
- Validates substituted values for security-relevant characters

**Execution:**
- Uses execute_sql_string() for proper multi-statement handling
- Ensures proper cleanup of global state and security context on errors

## Parameters / Member Variables
- `extensionOid`: OID of the extension being installed/updated
- `control`: Extension control file containing metadata and configuration
- `from_version`: Source version for updates (NULL for new installations)
- `version`: Target version to install/update to
- `requiredSchemas`: List of schemas for prerequisite extensions (must match control->requires)
- `schemaName`: Name of the target schema for the extension
- `schemaOid`: OID of the target schema

## Dependencies
- Functions called/Symbols referenced:
  - superuser (checks if current user is superuser)
  - extension_is_trusted (determines if extension can be installed by non-superuser)
  - get_extension_script_filename (constructs script file path)
  - read_extension_script_file (reads and converts script file)
  - execute_sql_string (executes the processed SQL)
  - SetUserIdAndSecContext/GetUserIdAndSecContext (security context management)
  - set_config_option (configures GUC variables)
  - DirectFunctionCall3Coll/DirectFunctionCall4Coll (text processing functions)
  - quote_identifier (SQL identifier quoting)
- Called from:
  - CreateExtensionInternal (new extension installation)
  - ApplyExtensionUpdates (extension updates)

## Notes and Other Information
- This is a static function within the extension.c module
- Implements PostgreSQL's trusted extension security model
- Uses PG_TRY/PG_FINALLY blocks to ensure proper cleanup on errors
- The function handles both installation (from_version == NULL) and update scenarios
- Variable substitution includes security validation to prevent SQL injection
- Search path setup ensures extension objects are created in the correct schema
- GUC variable management reduces noise during script execution
- The function is critical for the extension system's security and proper operation