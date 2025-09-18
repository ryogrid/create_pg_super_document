# parseServiceInfo

## Location
[src/interfaces/libpq/fe-connect.c:5492-5559](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L5492-L5559)

## Overview
Looks up and parses PostgreSQL service configuration from service files, loading connection options from the specified service definition into the options array.

## Definition
```c
static int parseServiceInfo(PQconninfoOption *options, PQExpBuffer errorMessage)
```

## Detailed Description
parseServiceInfo is an internal libpq function that implements PostgreSQL's service-based connection configuration system. The function allows users to define connection parameters in configuration files and reference them by service name, simplifying connection string management. It follows a hierarchical search pattern through multiple configuration file locations, providing flexibility for both user-specific and system-wide service definitions.

The function first checks for a service name in the options array or the PGSERVICE environment variable. If a service is specified, it searches for service definition files in this order: the file specified by PGSERVICEFILE environment variable, ~/.pg_service.conf in the user's home directory, and finally the system-wide pg_service.conf file. The search continues until a matching service definition is found or all locations are exhausted.

## Parameters / Member Variables
- `options`: Array of PQconninfoOption structures where service-defined connection parameters will be stored
- `errorMessage`: PQExpBuffer for accumulating error messages if the service lookup fails

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_getval](../c/conninfo_getval.md)
  - [parseServiceFile](parseServiceFile.md)
  - [pqGetHomeDirectory](pqGetHomeDirectory.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - strlcpy
  - getenv, stat, snprintf (standard C library functions)
  - MAXPGPATH, SYSCONFDIR (constants)
- Called from (representative examples):
  - internalPQconninfoOption
  - [conninfo_add_defaults](../c/conninfo_add_defaults.md)

## Notes and Other Information
- Returns 0 on success, 3 if the specified service is not found, or other nonzero values on failure
- Service name can be specified via the "service" connection parameter or PGSERVICE environment variable
- Searches configuration files in order: PGSERVICEFILE → ~/.pg_service.conf → system pg_service.conf
- The function handles the special case of PGSERVICE before other environment variable processing
- If no service is specified (service == NULL), the function returns immediately with success
- Static function (internal to libpq) used during connection option processing
- Part of PostgreSQL's centralized connection configuration system
- The search stops at the first file where the service definition is found