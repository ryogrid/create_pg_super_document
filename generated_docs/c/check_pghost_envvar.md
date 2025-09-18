# check_pghost_envvar

## Location
src/bin/pg_upgrade/server.c: 358 - 388

## Overview
Validates that PGHOST and PGHOSTADDR environment variables do not point to non-local servers during pg_upgrade operations.

## Definition
```c
void check_pghost_envvar(void)
```

## Detailed Description
This function performs a security check to ensure that pg_upgrade operations only connect to local PostgreSQL servers. It examines the PGHOST and PGHOSTADDR environment variables to verify they contain local server addresses. The function uses libpq's PQconndefaults() to get the list of valid connection parameters and their associated environment variables, then checks if any PGHOST or PGHOSTADDR values point to remote servers. This prevents accidental connections to remote databases during upgrade operations.

## Parameters / Member Variables
- This function takes no parameters (void)

## Dependencies
- Functions called/Symbols referenced:
  - PQconndefaults
  - [PQconninfoFree](../P/PQconninfoFree.md)
  - getenv
  - [is_unixsock_path](../i/is_unixsock_path.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [setup](../s/setup.md) (in pg_upgrade.c)

## Notes and Other Information
- Accepts local values: "localhost", "127.0.0.1", "::1", and Unix socket paths
- Prevents remote connections during upgrade for security and data integrity
- Uses libpq's connection defaults system to identify relevant environment variables
- Terminates pg_upgrade with fatal error if non-local server values are found
- Part of pg_upgrade's safety mechanisms to prevent data corruption
- Located in src/bin/pg_upgrade/server.c:358-388