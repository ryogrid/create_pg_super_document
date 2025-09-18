# conninfo_add_defaults

## Location
src/interfaces/libpq/fe-connect.c: 6187 - 6321

## Overview
Populates unspecified connection options with default values obtained from service files, environment variables, and compiled-in defaults.

## Definition


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
  - pg_fe_getauthname
  - getenv (standard C library function)
  - strcmp, strdup, free (standard C library functions)
- Called from (representative examples):
  - internalPQconninfoOption (src/interfaces/libpq/fe-connect.c:418)
  - PQconndefaults (src/interfaces/libpq/fe-connect.c:1895)
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