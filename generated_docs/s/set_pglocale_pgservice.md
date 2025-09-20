# set_pglocale_pgservice

## Location
[src/common/exec.c:448-497](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/exec.c#L448-L497)

## Overview
Sets up application-specific locale and service directory paths for PostgreSQL programs, configuring internationalization and system configuration directories.

## Definition

```c
void
set_pglocale_pgservice(const char *argv0, const char *app)
```
## Detailed Description
This function initializes locale settings and service directories for PostgreSQL applications. It performs several key tasks: sets the application locale using  (except for backend processes), determines the executable path, configures NLS (Native Language Support) paths for internationalization, and sets up environment variables for libpq configuration. The function handles both locale configuration for message translation and system configuration directory setup, ensuring that PostgreSQL utilities can find their resource files regardless of installation location.

## Parameters / Member Variables
- : The value of argv[0] from main(), used to determine the executable's location
- : Application name identifier, used to determine if this is a backend process and for NLS domain setup

## Dependencies
- Functions called/Symbols referenced:
  -  - Macro to get text domain for postgres backend
  -  - Standard library function to set program locale
  -  - Determines the full path of the current executable
  -  - Constructs path to locale files for NLS
  -  - Associates message domain with directory
  -  - Sets default domain for message lookup
  -  - Sets environment variables for PGLOCALEDIR and PGSYSCONFDIR
  -  - Constructs path to system configuration directory
- Called from (representative examples):
  -  functions in virtually all PostgreSQL utilities and programs
  -  (src/test/regress/pg_regress.c:2106)

## Notes and Other Information
- [Backend](../B/Backend.md) processes (postgres) skip locale setting to avoid conflicts with postmaster locale management
- The function includes detailed commentary about multithreading considerations in frontend programs
- Only sets environment variables if they are not already defined (using setenv with overwrite=0)
- NLS configuration is conditionally compiled based on ENABLE_NLS
- Essential initialization function called by virtually all PostgreSQL programs during startup
- Handles both development and installed configurations by using executable location to find resource directories
- Part of PostgreSQL's portable runtime environment setup