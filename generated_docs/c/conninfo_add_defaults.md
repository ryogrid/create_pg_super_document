# conninfo_add_defaults

## Location
[src/interfaces/libpq/fe-connect.c:6187-6321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6187-L6321)

## Overview
Populates unspecified connection options with default values obtained from service files, environment variables, and compiled-in defaults.

## Definition

```c
static bool
conninfo_add_defaults(PQconninfoOption *options, PQExpBuffer errorMessage)
```
## Detailed Description
This function implements a sophisticated default value resolution system for PostgreSQL connection parameters. It follows a specific precedence hierarchy to determine default values for any connection options that weren't explicitly specified by the user.

The default resolution process follows this order:
1. **Service File Lookup**: Uses  to obtain defaults from PostgreSQL service files
2. **Environment Variables**: Checks each option's associated environment variable (stored in )
3. **Legacy Environment Variables**: Special handling for deprecated  environment variable
4. **Compiled Defaults**: Uses built-in default values from 
5. **Special Cases**: Custom logic for specific parameters like "user" (uses system authentication name)

The function also implements advanced SSL configuration logic, automatically upgrading the sslmode to "verify-full" when sslrootcert="system" is specified but no explicit sslmode was provided.

## Parameters / Member Variables
- : Array of PQconninfoOption structures to populate with default values
- : Optional buffer for storing error messages (NULL if error reporting not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [parseServiceInfo](../p/parseServiceInfo.md)
  - [libpq_append_error](../l/libpq_append_error.md)  
  - [pg_fe_getauthname](../p/pg_fe_getauthname.md)
  - getenv (standard C library function)
  - strcmp, strdup, free (standard C library functions)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:418)
  - [PQconndefaults](../P/PQconndefaults.md) (src/interfaces/libpq/fe-connect.c:1895)
  - [conninfo_parse](conninfo_parse.md) (src/interfaces/libpq/fe-connect.c:5999)
  - [conninfo_array_parse](conninfo_array_parse.md) (src/interfaces/libpq/fe-connect.c:6165)
  - [conninfo_uri_parse](conninfo_uri_parse.md) (src/interfaces/libpq/fe-connect.c:6343)

## Notes and Other Information
- This is a static function, internal to the fe-connect.c file
- Function never fails due to missing defaults - it simply leaves values as NULL if no default is available
- Failure only occurs on memory allocation errors or service file parsing errors
- Implements backward compatibility for the deprecated PGREQUIRESSL environment variable
- Special SSL security enhancement: when sslrootcert="system", automatically sets sslmode="verify-full" for stronger security
- The "user" parameter gets special treatment by attempting to determine the system authentication name
- Service file parsing errors are only reported if an errorMessage buffer is provided
- Environment variable names are stored in the PQconninfoOption structure's envvar field
- Returns true on success, false only on allocation errors or service parsing failures

## Simplified Source
```c
static bool conninfo_add_defaults(PQconninfoOption *options, PQExpBuffer errorMessage) {
    PQconninfoOption *option;
    PQconninfoOption *sslmode_default = NULL, *sslrootcert = NULL;
    char *tmp;

    // Parse service file for defaults
    if (parseServiceInfo(options, errorMessage) != 0 && errorMessage)
        return false;

    // Fill in defaults for each option
    for (option = options; option->keyword != NULL; option++) {
        if (strcmp(option->keyword, "sslrootcert") == 0)
            sslrootcert = option;

        if (option->val != NULL)
            continue; // Already has value

        // Try environment variable
        if (option->envvar != NULL) {
            if ((tmp = getenv(option->envvar)) != NULL) {
                option->val = strdup(tmp);
                if (!option->val) {
                    if (errorMessage)
                        libpq_append_error(errorMessage, "out of memory");
                    return false;
                }
                continue;
            }
        }

        // Handle deprecated PGREQUIRESSL for sslmode
        if (strcmp(option->keyword, "sslmode") == 0) {
            const char *requiresslenv = getenv("PGREQUIRESSL");
            if (requiresslenv != NULL && requiresslenv[0] == '1') {
                option->val = strdup("require");
                if (!option->val) {
                    if (errorMessage)
                        libpq_append_error(errorMessage, "out of memory");
                    return false;
                }
                continue;
            }
            sslmode_default = option;
        }

        // Use compiled-in default
        if (option->compiled != NULL) {
            option->val = strdup(option->compiled);
            if (!option->val) {
                if (errorMessage)
                    libpq_append_error(errorMessage, "out of memory");
                return false;
            }
            continue;
        }

        // Special case for user parameter
        if (strcmp(option->keyword, "user") == 0) {
            option->val = pg_fe_getauthname(NULL);
            continue;
        }
    }

    // SSL security enhancement: system cert + no explicit sslmode = verify-full
    if (sslmode_default && sslrootcert) {
        if (sslrootcert->val && strcmp(sslrootcert->val, "system") == 0) {
            free(sslmode_default->val);
            sslmode_default->val = strdup("verify-full");
            if (!sslmode_default->val) {
                if (errorMessage)
                    libpq_append_error(errorMessage, "out of memory");
                return false;
            }
        }
    }

    return true;
}
```