# CheckMyDatabase

## Location
[src/backend/utils/init/postinit.c:313-518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L313-L518)

## Overview
CheckMyDatabase is a static function that validates and configures database-specific settings by fetching and processing information from the pg_database catalog entry for the current database.

## Definition
static void CheckMyDatabase(const char *name, bool am_superuser, bool override_allow_connections)

## Detailed Description
This function performs comprehensive validation and setup for the current database during backend initialization. It handles multiple critical tasks:

1. **Database Validation**: Fetches the pg_database entry and verifies the database still exists and matches expectations
2. **Connection Permission Checks**: Validates that the database allows connections and the user has appropriate privileges
3. **Connection Limit Enforcement**: Checks and enforces database-specific connection limits
4. **Encoding Configuration**: Sets up database encoding and client encoding settings
5. **Locale Configuration**: Configures LC_COLLATE and LC_CTYPE settings, handles both libc and ICU providers
6. **Collation Version Validation**: Checks for collation version mismatches and warns if detected

The function supports different locale providers (builtin, ICU, libc) and handles locale validation and setup accordingly. It also performs security checks unless running in standalone mode.

## Parameters / Member Variables
- : The expected database name to validate against
- : Boolean indicating if the current user is a superuser (affects permission checks)
- : Boolean allowing background processes to bypass connection restrictions

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)/ReleaseSysCache (for pg_database lookup)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)/NameStr (for data conversion)
  - [object_aclcheck](../o/object_aclcheck.md) (for privilege checking)
  - AmRegularBackendProcess/CountDBConnections (for connection limit checks)
  - [SetDatabaseEncoding](../S/SetDatabaseEncoding.md)/GetDatabaseEncodingName (for encoding setup)
  - [SetConfigOption](../S/SetConfigOption.md) (for GUC configuration)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)/SysCacheGetAttr (for attribute retrieval)
  - TextDatumGetCString (for text conversion)
  - [pg_perm_setlocale](../p/pg_perm_setlocale.md) (for locale setting)
  - [builtin_validate_locale](../b/builtin_validate_locale.md) (for builtin locale validation)
  - [make_icu_collator](../m/make_icu_collator.md) (for ICU collation setup)
  - [get_collation_actual_version](../g/get_collation_actual_version.md) (for version checking)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (for memory management)
  - [quote_identifier](../q/quote_identifier.md) (for error message formatting)
- Called from:
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:1206)

## Notes and Other Information
- This is a static function, only accessible within postinit.c
- Performs fatal error reporting for critical validation failures
- Bypasses connection checks when not under postmaster (standalone mode)
- Handles race condition in connection limit checking (documented as acceptable)
- Supports multiple collation providers: builtin, ICU, and libc
- Sets global variables like database_ctype_is_c and default_locale
- Warns about collation version mismatches rather than failing
- Critical for ensuring database compatibility and proper locale setup
- Part of the database initialization sequence after authentication

## Simplified Source

```c
// Simplified version of CheckMyDatabase
static void CheckMyDatabase(const char *name, bool am_superuser, bool override_allow_connections) {
    HeapTuple tup;
    Form_pg_database dbform;
    Datum datum;
    bool isnull;
    char *collate, *ctype, *datlocale;

    // Fetch database entry from pg_database catalog
    tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
    if (!HeapTupleIsValid(tup)) {
        elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
    }
    dbform = (Form_pg_database) GETSTRUCT(tup);

    // Paranoia check: ensure database name matches
    if (strcmp(name, NameStr(dbform->datname)) != 0) {
        ereport(FATAL, "database has disappeared from pg_database");
    }

    // Connection permission checks (only when under postmaster)
    if (IsUnderPostmaster) {
        // Check if database allows connections
        if (!dbform->datallowconn && !override_allow_connections) {
            ereport(FATAL, "database is not currently accepting connections");
        }

        // Check user privilege to connect
        if (!am_superuser && !override_allow_connections &&
            object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CONNECT) != ACLCHECK_OK) {
            ereport(FATAL, "permission denied for database");
        }

        // Check connection limit
        if (dbform->datconnlimit >= 0 && AmRegularBackendProcess() &&
            !am_superuser && CountDBConnections(MyDatabaseId) > dbform->datconnlimit) {
            ereport(FATAL, "too many connections for database");
        }
    }

    // Set up database encoding
    SetDatabaseEncoding(dbform->encoding);
    SetConfigOption("server_encoding", GetDatabaseEncodingName(), PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT);
    SetConfigOption("client_encoding", GetDatabaseEncodingName(), PGC_BACKEND, PGC_S_DYNAMIC_DEFAULT);

    // Get and set locale information
    datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datcollate);
    collate = TextDatumGetCString(datum);
    datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datctype);
    ctype = TextDatumGetCString(datum);

    // Set system locales
    if (pg_perm_setlocale(LC_COLLATE, collate) == NULL) {
        ereport(FATAL, "database locale is incompatible with operating system");
    }
    if (pg_perm_setlocale(LC_CTYPE, ctype) == NULL) {
        ereport(FATAL, "database locale is incompatible with operating system");
    }

    // Set ctype flag for optimization
    if (strcmp(ctype, "C") == 0 || strcmp(ctype, "POSIX") == 0) {
        database_ctype_is_c = true;
    }

    // Handle different locale providers
    default_locale.provider = dbform->datlocprovider;
    default_locale.deterministic = true;

    if (dbform->datlocprovider == COLLPROVIDER_BUILTIN) {
        datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datlocale);
        datlocale = TextDatumGetCString(datum);
        builtin_validate_locale(dbform->encoding, datlocale);
        default_locale.info.builtin.locale = MemoryContextStrdup(TopMemoryContext, datlocale);
    } else if (dbform->datlocprovider == COLLPROVIDER_ICU) {
        datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datlocale);
        datlocale = TextDatumGetCString(datum);

        // Get ICU rules if present
        datum = SysCacheGetAttr(DATABASEOID, tup, Anum_pg_database_daticurules, &isnull);
        char *icurules = isnull ? NULL : TextDatumGetCString(datum);

        make_icu_collator(datlocale, icurules, &default_locale);
    }

    // Check collation version and warn if mismatch
    datum = SysCacheGetAttr(DATABASEOID, tup, Anum_pg_database_datcollversion, &isnull);
    if (!isnull) {
        char *collversionstr = TextDatumGetCString(datum);
        char *locale = (dbform->datlocprovider == COLLPROVIDER_LIBC) ? collate : datlocale;
        char *actual_versionstr = get_collation_actual_version(dbform->datlocprovider, locale);

        if (actual_versionstr && strcmp(actual_versionstr, collversionstr) != 0) {
            ereport(WARNING, "database has a collation version mismatch");
        }
    }

    ReleaseSysCache(tup);
}
```

Key simplifications made:
- Condensed error messages while preserving essential information
- Simplified complex conditional logic for readability
- Abstracted detailed error message formatting into concise versions
- Consolidated similar locale provider handling patterns
- Removed extensive error detail and hint messages for brevity
- Maintained all critical validation and setup operations
- Reduced from ~200 lines to ~80 lines while preserving core functionality
- Kept all essential security checks and configuration steps