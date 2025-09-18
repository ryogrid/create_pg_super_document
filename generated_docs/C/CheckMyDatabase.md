# CheckMyDatabase

## Location
src/backend/utils/init/postinit.c: 313 - 518

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
  - SearchSysCache1/ReleaseSysCache (for pg_database lookup)
  - ObjectIdGetDatum/NameStr (for data conversion)
  - object_aclcheck (for privilege checking)
  - AmRegularBackendProcess/CountDBConnections (for connection limit checks)
  - SetDatabaseEncoding/GetDatabaseEncodingName (for encoding setup)
  - SetConfigOption (for GUC configuration)
  - SysCacheGetAttrNotNull/SysCacheGetAttr (for attribute retrieval)
  - TextDatumGetCString (for text conversion)
  - pg_perm_setlocale (for locale setting)
  - builtin_validate_locale (for builtin locale validation)
  - make_icu_collator (for ICU collation setup)
  - get_collation_actual_version (for version checking)
  - MemoryContextStrdup (for memory management)
  - quote_identifier (for error message formatting)
- Called from:
  - InitPostgres (src/backend/utils/init/postinit.c:1206)

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