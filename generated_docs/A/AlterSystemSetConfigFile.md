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
  - [superuser](../s/superuser.md)
  - [pg_parameter_aclcheck](../p/pg_parameter_aclcheck.md)
  - [GetUserId](../G/GetUserId.md)
  - [find_option](../f/find_option.md)
  - [parse_and_validate_value](../p/parse_and_validate_value.md)
  - [valid_custom_variable_name](../v/valid_custom_variable_name.md)
  - [assignable_custom_variable_name](../a/assignable_custom_variable_name.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [AllocateFile](AllocateFile.md)
  - [FreeFile](../F/FreeFile.md)
  - ParseConfigFp
  - [replace_auto_config_value](../r/replace_auto_config_value.md)
  - [write_auto_conf_file](../w/write_auto_conf_file.md)
  - InvokeObjectPostAlterHookArgStr
  - [BasicOpenFile](../B/BasicOpenFile.md)
  - [durable_rename](../d/durable_rename.md)
  - FreeConfigVariables
  - [guc_free](../g/guc_free.md)
- Data structures used:
  - [AlterSystemStmt](AlterSystemStmt.md)
  - [ConfigVariable](../C/ConfigVariable.md)
  - [config_generic](../c/config_generic.md)
  - config_var_val
  - [AclResult](AclResult.md)
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

## Simplified Source

```c
void AlterSystemSetConfigFile(AlterSystemStmt *altersysstmt) {
    char *name, *value;
    bool resetall = false;
    ConfigVariable *head = NULL, *tail = NULL;

    // Extract statement arguments
    name = altersysstmt->setstmt->name;

    if (!AllowAlterSystem)
        ereport(ERROR, "ALTER SYSTEM is not allowed in this environment");

    // Determine operation type and extract value
    switch (altersysstmt->setstmt->kind) {
        case VAR_SET_VALUE:
            value = ExtractSetVariableArgs(altersysstmt->setstmt);
            break;
        case VAR_SET_DEFAULT:
        case VAR_RESET:
            value = NULL;
            break;
        case VAR_RESET_ALL:
            value = NULL;
            resetall = true;
            break;
    }

    // Permission checks
    if (!superuser()) {
        if (resetall)
            ereport(ERROR, "permission denied to perform ALTER SYSTEM RESET ALL");
        else {
            AclResult aclresult = pg_parameter_aclcheck(name, GetUserId(), ACL_ALTER_SYSTEM);
            if (aclresult != ACLCHECK_OK)
                ereport(ERROR, "permission denied to set parameter");
        }
    }

    // Parameter validation (unless RESET ALL)
    if (!resetall) {
        struct config_generic *record = find_option(name, false, true, DEBUG5);

        if (record != NULL) {
            // Check if parameter can be set in config files
            if ((record->context == PGC_INTERNAL) ||
                (record->flags & GUC_DISALLOW_IN_FILE) ||
                (record->flags & GUC_DISALLOW_IN_AUTO_FILE))
                ereport(ERROR, "parameter cannot be changed");

            // Validate value if provided
            if (value) {
                if (!parse_and_validate_value(record, name, value, PGC_S_FILE, ERROR, &newval, &newextra))
                    ereport(ERROR, "invalid value for parameter");
            }
        } else {
            // Unknown parameter - validate custom variable name
            if (value || !valid_custom_variable_name(name))
                (void) assignable_custom_variable_name(name, false, ERROR);
        }

        // Reject values with newlines
        if (value && strchr(value, '\n'))
            ereport(ERROR, "parameter value must not contain a newline");
    }

    // File manipulation with locking
    LWLockAcquire(AutoFileLock, LW_EXCLUSIVE);

    // Read existing config file (unless RESET ALL)
    if (!resetall) {
        if (stat(AutoConfFileName, &st) == 0) {
            FILE *infile = AllocateFile(AutoConfFileName, "r");
            if (!ParseConfigFp(infile, AutoConfFileName, CONF_FILE_START_DEPTH, LOG, &head, &tail))
                ereport(ERROR, "could not parse contents of file");
            FreeFile(infile);
        }

        // Update configuration with new value
        replace_auto_config_value(&head, &tail, name, value);
    }

    // Invoke post-alter hook
    InvokeObjectPostAlterHookArgStr(ParameterAclRelationId, name, ACL_ALTER_SYSTEM,
                                    altersysstmt->setstmt->kind, false);

    // Atomic file update using temporary file
    PG_TRY();
    {
        // Write to temporary file
        Tmpfd = BasicOpenFile(AutoConfTmpFileName, O_CREAT | O_RDWR | O_TRUNC);
        write_auto_conf_file(Tmpfd, AutoConfTmpFileName, head);
        close(Tmpfd);

        // Atomically replace original file
        durable_rename(AutoConfTmpFileName, AutoConfFileName, ERROR);
    }
    PG_CATCH();
    {
        // Cleanup on error
        if (Tmpfd >= 0) close(Tmpfd);
        (void) unlink(AutoConfTmpFileName);
        PG_RE_THROW();
    }
    PG_END_TRY();

    FreeConfigVariables(head);
    LWLockRelease(AutoFileLock);
}
```