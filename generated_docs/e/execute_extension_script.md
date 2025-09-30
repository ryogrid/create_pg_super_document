# execute_extension_script

## Location
[src/backend/commands/extension.c:870-1142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/extension.c#L870-L1142)

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
  - [superuser](../s/superuser.md) (checks if current user is superuser)
  - [extension_is_trusted](extension_is_trusted.md) (determines if extension can be installed by non-superuser)
  - [get_extension_script_filename](../g/get_extension_script_filename.md) (constructs script file path)
  - [read_extension_script_file](../r/read_extension_script_file.md) (reads and converts script file)
  - [execute_sql_string](execute_sql_string.md) (executes the processed SQL)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)/GetUserIdAndSecContext (security context management)
  - [set_config_option](../s/set_config_option.md) (configures GUC variables)
  - [DirectFunctionCall3Coll](../D/DirectFunctionCall3Coll.md)/DirectFunctionCall4Coll (text processing functions)
  - [quote_identifier](../q/quote_identifier.md) (SQL identifier quoting)
- Called from:
  - [CreateExtensionInternal](../C/CreateExtensionInternal.md) (new extension installation)
  - [ApplyExtensionUpdates](../A/ApplyExtensionUpdates.md) (extension updates)

## Notes and Other Information
- This is a static function within the extension.c module
- Implements PostgreSQL's trusted extension security model
- Uses PG_TRY/PG_FINALLY blocks to ensure proper cleanup on errors
- The function handles both installation (from_version == NULL) and update scenarios
- [Variable](../V/Variable.md) substitution includes security validation to prevent SQL injection
- Search path setup ensures extension objects are created in the correct schema
- GUC variable management reduces noise during script execution
- The function is critical for the extension system's security and proper operation

## Simplified Source

```c
static void execute_extension_script(Oid extensionOid, ExtensionControlFile *control,
                                    const char *from_version, const char *version,
                                    List *requiredSchemas, const char *schemaName,
                                    Oid schemaOid) {
    bool switch_to_superuser = false;
    Oid save_userid = 0;
    int save_sec_context = 0;
    int save_nestlevel;

    // Security: Check if superuser is required
    if (control->superuser && !superuser()) {
        if (extension_is_trusted(control)) {
            switch_to_superuser = true;
        } else {
            ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                           errmsg("permission denied to %s extension \"%s\"",
                                  from_version ? "update" : "create", control->name)));
        }
    }

    // Get script filename and log execution
    char *filename = get_extension_script_filename(control, from_version, version);
    elog(DEBUG1, "executing extension script for \"%s\" %s", control->name,
         from_version ? "update" : "installation");

    // Switch to superuser if needed for trusted extensions
    if (switch_to_superuser) {
        GetUserIdAndSecContext(&save_userid, &save_sec_context);
        SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
                              save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    }

    // Configure GUC variables to reduce noise and improve security
    save_nestlevel = NewGUCNestLevel();
    if (client_min_messages < WARNING) {
        set_config_option("client_min_messages", "warning", PGC_USERSET,
                         PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    }
    if (log_min_messages < WARNING) {
        set_config_option_ext("log_min_messages", "warning", PGC_SUSET,
                             PGC_S_SESSION, BOOTSTRAP_SUPERUSERID,
                             GUC_ACTION_SAVE, true, 0, false);
    }
    if (check_function_bodies) {
        set_config_option("check_function_bodies", "off", PGC_USERSET,
                         PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    }

    // Set up search path: target schema + required schemas + pg_temp
    StringInfoData pathbuf;
    initStringInfo(&pathbuf);
    appendStringInfoString(&pathbuf, quote_identifier(schemaName));

    foreach(lc, requiredSchemas) {
        Oid reqschema = lfirst_oid(lc);
        char *reqname = get_namespace_name(reqschema);
        if (reqname && strcmp(reqname, "pg_catalog") != 0) {
            appendStringInfo(&pathbuf, ", %s", quote_identifier(reqname));
        }
    }
    appendStringInfoString(&pathbuf, ", pg_temp");
    set_config_option("search_path", pathbuf.data, PGC_USERSET,
                     PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    // Set extension creation context
    creating_extension = true;
    CurrentExtensionObject = extensionOid;

    PG_TRY();
    {
        // Read and process script file
        char *c_sql = read_extension_script_file(control, filename);
        Datum t_sql = CStringGetTextDatum(c_sql);

        // Remove \echo commands
        t_sql = DirectFunctionCall4Coll(textregexreplace, C_COLLATION_OID,
                                       t_sql, CStringGetTextDatum("^\\\\\\\\echo.*$"),
                                       CStringGetTextDatum(""), CStringGetTextDatum("ng"));

        // Substitute @extowner@ with actual username
        if (strstr(c_sql, "@extowner@")) {
            Oid uid = switch_to_superuser ? save_userid : GetUserId();
            const char *userName = GetUserNameFromId(uid, false);
            const char *qUserName = quote_identifier(userName);

            t_sql = DirectFunctionCall3Coll(replace_text, C_COLLATION_OID,
                                           t_sql, CStringGetTextDatum("@extowner@"),
                                           CStringGetTextDatum(qUserName));

            // Security check for problematic characters
            if (strpbrk(userName, "\"$'\\\\")) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               errmsg("invalid character in extension owner")));
            }
        }

        // Substitute @extschema@ for non-relocatable extensions
        if (!control->relocatable) {
            const char *qSchemaName = quote_identifier(schemaName);
            Datum old = t_sql;

            t_sql = DirectFunctionCall3Coll(replace_text, C_COLLATION_OID,
                                           t_sql, CStringGetTextDatum("@extschema@"),
                                           CStringGetTextDatum(qSchemaName));

            if (t_sql != old && strpbrk(schemaName, "\"$'\\\\")) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                               errmsg("invalid character in extension schema")));
            }
        }

        // Substitute @extschema:extension_name@ for required extensions
        forboth(lc, control->requires, lc2, requiredSchemas) {
            char *reqextname = (char *) lfirst(lc);
            Oid reqschema = lfirst_oid(lc2);
            char *reqSchemaName = get_namespace_name(reqschema);
            char *repltoken = psprintf("@extschema:%s@", reqextname);

            t_sql = DirectFunctionCall3Coll(replace_text, C_COLLATION_OID,
                                           t_sql, CStringGetTextDatum(repltoken),
                                           CStringGetTextDatum(quote_identifier(reqSchemaName)));
        }

        // Substitute MODULE_PATHNAME if specified
        if (control->module_pathname) {
            t_sql = DirectFunctionCall3Coll(replace_text, C_COLLATION_OID,
                                           t_sql, CStringGetTextDatum("MODULE_PATHNAME"),
                                           CStringGetTextDatum(control->module_pathname));
        }

        // Execute the processed SQL script
        c_sql = text_to_cstring(DatumGetTextPP(t_sql));
        execute_sql_string(c_sql);
    }
    PG_FINALLY();
    {
        // Clean up extension creation context
        creating_extension = false;
        CurrentExtensionObject = InvalidOid;
    }
    PG_END_TRY();

    // Restore GUC variables and authentication state
    AtEOXact_GUC(true, save_nestlevel);
    if (switch_to_superuser) {
        SetUserIdAndSecContext(save_userid, save_sec_context);
    }
}
```