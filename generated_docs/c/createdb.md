# createdb

## Location
[src/backend/commands/dbcommands.c:670-1556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L670-L1556)

## Overview
createdb is the main function that implements the CREATE DATABASE SQL command, responsible for creating a new PostgreSQL database by copying from a template database.

## Definition

```c
struct stat st;
```
## Detailed Description
This comprehensive function handles all aspects of database creation including option parsing, validation, permission checking, and the actual database copying process. It supports multiple creation strategies (WAL logging vs file copy) and handles complex locale, encoding, and tablespace configurations.

The function parses CREATE DATABASE statement options, validates permissions and compatibility with the template database, ensures proper encoding/locale matching, handles tablespace assignments, and coordinates the actual database creation using either WAL logging or file copying strategies.

Key operations include: option parsing and validation, permission and ownership checks, template database compatibility verification, encoding and locale validation, tablespace resolution, conflict detection, and database copying with proper error cleanup mechanisms.

## Parameters / Member Variables
- : ParseState for error reporting and parsing context
- : CreatedbStmt containing the parsed CREATE DATABASE statement with all specified options

## Dependencies
- Functions called/Symbols referenced:
  - [get_db_info](../g/get_db_info.md), database_is_invalid_oid, CountOtherDBBackends
  - [check_encoding_locale_matches](check_encoding_locale_matches.md), check_locale, builtin_validate_locale, icu_validate_locale
  - [have_createdb_privilege](../h/have_createdb_privilege.md), check_can_set_role, object_ownercheck
  - [get_tablespace_oid](../g/get_tablespace_oid.md), GetDatabasePath, get_database_oid
  - [CreateDatabaseUsingWalLog](../C/CreateDatabaseUsingWalLog.md), CreateDatabaseUsingFileCopy
  - [createdb_failure_callback](createdb_failure_callback.md), ForceSyncCommit
  - Various catalog operations: CatalogTupleInsert, recordDependencyOnOwner
- Called from (representative examples):
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md) (main SQL command processing)

## Notes and Other Information
- Supports two database creation strategies: CREATEDB_WAL_LOG (default) and CREATEDB_FILE_COPY
- Enforces strict compatibility requirements between new database and template (encoding, locale, collation) unless using template0
- Implements comprehensive error handling with cleanup callbacks to handle failures during the creation process
- Manages complex locale provider scenarios (libc, ICU, builtin) with proper validation and canonicalization
- Handles both explicitly assigned database OIDs and automatic OID generation with conflict detection
- template0 is treated specially as it's assumed to contain no collation-dependent data, allowing different encodings/locales
- Uses ShareLock on template database to prevent concurrent modifications during copying

## Simplified Source

```c
Oid createdb(ParseState *pstate, const CreatedbStmt *stmt) {
    // Variable declarations for options and template info
    Oid src_dboid, dst_deftablespace, dboid = InvalidOid;
    char *dbname = stmt->dbname;
    char *dbowner = NULL, *dbtemplate = NULL;
    char *dbcollate = NULL, *dbctype = NULL, *dblocale = NULL;
    int encoding = -1;
    bool dbistemplate = false, dballowconnections = true;
    int dbconnlimit = DATCONNLIMIT_UNLIMITED;
    CreateDBStrategy dbstrategy = CREATEDB_WAL_LOG;

    // Parse CREATE DATABASE options
    foreach(option, stmt->options) {
        DefElem *defel = (DefElem *) lfirst(option);

        if (strcmp(defel->defname, "owner") == 0)
            dbowner = defGetString(defel);
        else if (strcmp(defel->defname, "template") == 0)
            dbtemplate = defGetString(defel);
        else if (strcmp(defel->defname, "encoding") == 0)
            encoding = get_encoding_from_option(defel);
        else if (strcmp(defel->defname, "locale") == 0)
            dblocale = dbcollate = dbctype = defGetString(defel);
        else if (strcmp(defel->defname, "lc_collate") == 0)
            dbcollate = defGetString(defel);
        else if (strcmp(defel->defname, "lc_ctype") == 0)
            dbctype = defGetString(defel);
        else if (strcmp(defel->defname, "tablespace") == 0)
            dst_deftablespace = get_tablespace_oid(defGetString(defel), false);
        else if (strcmp(defel->defname, "strategy") == 0)
            dbstrategy = parse_creation_strategy(defGetString(defel));
        // ... handle other options
    }

    // Resolve database owner
    Oid datdba = dbowner ? get_role_oid(dbowner, false) : GetUserId();

    // Permission checks
    if (!have_createdb_privilege())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to create database")));
    check_can_set_role(GetUserId(), datdba);

    // Get template database info (default to template1)
    if (!dbtemplate)
        dbtemplate = "template1";

    if (!get_db_info(dbtemplate, ShareLock, &src_dboid, &src_owner,
                     &src_encoding, &src_istemplate, &src_allowconn,
                     &src_hasloginevt, &src_frozenxid, &src_minmxid,
                     &src_deftablespace, &src_collate, &src_ctype,
                     &src_locale, &src_icurules, &src_locprovider,
                     &src_collversion))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_DATABASE),
                       errmsg("template database \"%s\" does not exist", dbtemplate)));

    // Check template database permissions
    if (!src_istemplate && !object_ownercheck(DatabaseRelationId, src_dboid, GetUserId()))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied to copy database \"%s\"", dbtemplate)));

    // Use template defaults for unspecified options
    if (encoding < 0) encoding = src_encoding;
    if (!dbcollate) dbcollate = src_collate;
    if (!dbctype) dbctype = src_ctype;
    if (!dblocale) dblocale = src_locale;

    // Validate encoding and locale compatibility
    validate_encoding_and_locale(encoding, dbcollate, dbctype, dblocale);

    // Check compatibility with template (unless using template0)
    if (strcmp(dbtemplate, "template0") != 0) {
        if (encoding != src_encoding)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("new encoding incompatible with template")));
        if (strcmp(dbcollate, src_collate) != 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("new collation incompatible with template")));
        // ... other compatibility checks
    }

    // Check for database name conflicts
    if (OidIsValid(get_database_oid(dbname, true)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_DATABASE),
                       errmsg("database \"%s\" already exists", dbname)));

    // Ensure no other backends are using the template
    if (CountOtherDBBackends(src_dboid, &notherbackends, &npreparedxacts))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_IN_USE),
                       errmsg("source database \"%s\" is being accessed by other users",
                              dbtemplate)));

    // Generate new database OID and create catalog entry
    Relation pg_database_rel = table_open(DatabaseRelationId, RowExclusiveLock);

    if (!OidIsValid(dboid)) {
        do {
            dboid = GetNewOidWithIndex(pg_database_rel, DatabaseOidIndexId,
                                      Anum_pg_database_oid);
        } while (check_db_file_conflict(dboid));
    }

    // Insert into pg_database catalog
    create_database_catalog_entry(pg_database_rel, dboid, dbname, datdba,
                                 encoding, dbcollate, dbctype, dblocale,
                                 dst_deftablespace, dbistemplate,
                                 dballowconnections, dbconnlimit);

    // Create database dependencies and invoke hooks
    recordDependencyOnOwner(DatabaseRelationId, dboid, datdba);
    copyTemplateDependencies(src_dboid, dboid);
    InvokeObjectPostCreateHook(DatabaseRelationId, dboid, 0);

    // Copy database contents using specified strategy
    PG_ENSURE_ERROR_CLEANUP(createdb_failure_callback, &fparms);
    {
        if (dbstrategy == CREATEDB_WAL_LOG)
            CreateDatabaseUsingWalLog(src_dboid, dboid, src_deftablespace,
                                     dst_deftablespace);
        else
            CreateDatabaseUsingFileCopy(src_dboid, dboid, src_deftablespace,
                                       dst_deftablespace);

        table_close(pg_database_rel, NoLock);
        ForceSyncCommit();
    }
    PG_END_ENSURE_ERROR_CLEANUP(createdb_failure_callback, &fparms);

    return dboid;
}
```