# AlterSystemSetConfigFile

## Location
[src/backend/utils/misc/guc.c:4610-4878](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4610-L4878)

## Overview
Executes the ALTER SYSTEM statement by updating PostgreSQL's automatic configuration file with new parameter values, handling validation, permissions, and atomic file operations.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function implements the core functionality for the ALTER SYSTEM command, which allows dynamic modification of PostgreSQL configuration parameters by updating the postgresql.auto.conf file. The function handles various ALTER SYSTEM operations including setting new values, resetting parameters to defaults, and resetting all parameters.

The function performs comprehensive validation including permission checks, parameter existence validation, value format verification, and ensures parameters are allowed to be set via configuration files. It uses atomic file operations to maintain crash safety - writing to a temporary file first, then atomically renaming it to replace the original.

Key features include:
- Permission validation using ACL system and superuser checks  
- Parameter validation against GUC flags and contexts
- Prevention of newline characters in values (unsupported by config file grammar)
- LWLock serialization to prevent concurrent modifications
- Crash-safe atomic file updates using temporary files
- Support for custom variables with proper name validation
- Integration with PostgreSQL's object access hooks

## Parameters / Member Variables
- : Pointer to AlterSystemStmt structure containing the parsed ALTER SYSTEM statement with parameter name, value, and operation type

## Dependencies
- Functions called/Symbols referenced:
  - [ExtractSetVariableArgs](../E/ExtractSetVariableArgs.md)
  - superuser
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md)
  - [GetUserId](../G/GetUserId.md)
  - find_option
  - parse_and_validate_value
  - [valid_custom_variable_name](../v/valid_custom_variable_name.md)
  - [assignable_custom_variable_name](../a/assignable_custom_variable_name.md)
  - LWLockAcquire
  - LWLockRelease
  - AllocateFile
  - FreeFile
  - ParseConfigFp
  - [replace_auto_config_value](../r/replace_auto_config_value.md)
  - [write_auto_conf_file](../w/write_auto_conf_file.md)
  - InvokeObjectPostAlterHookArgStr
  - BasicOpenFile
  - [durable_rename](../d/durable_rename.md)
  - FreeConfigVariables
  - [guc_free](../g/guc_free.md)
- Data structures used:
  - AlterSystemStmt
  - ConfigVariable
  - config_generic
  - config_var_val
  - AclResult
- Constants referenced:
  - VAR_SET_VALUE, VAR_SET_DEFAULT, VAR_RESET, VAR_RESET_ALL
  - PGC_INTERNAL, PGC_STRING, PGC_S_FILE
  - GUC_DISALLOW_IN_FILE, GUC_DISALLOW_IN_AUTO_FILE
  - ACL_ALTER_SYSTEM
  - PG_AUTOCONF_FILENAME
  - CONF_FILE_START_DEPTH
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - EmitWarningsOnPlaceholders

## Notes and Other Information
- Uses AutoFileLock (LWLock) to serialize access to the configuration file across backends
- Implements crash safety through atomic file operations using temporary files and durable_rename
- Supports RESET ALL operation which creates an empty configuration file
- Validates that parameter values don't contain newlines (not supported by config file grammar)  
- Performs permission checks using both superuser() and ACL system for fine-grained access control
- Integrates with PostgreSQL's object access hook system for auditing and additional security checks
- Handles custom variables by validating names against reserved prefixes and extension policies
- Uses PG_TRY/PG_CATCH blocks for proper cleanup of temporary files on errors
- The operation is non-transactional - changes persist even if the containing transaction is rolled back