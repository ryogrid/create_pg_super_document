# stop_postmaster_atexit

## Location
[src/bin/pg_upgrade/server.c:191-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/server.c#L191-L197)

## Overview
A cleanup function registered as an atexit handler to ensure the postmaster process is properly stopped when pg_upgrade exits.

## Definition
```c
static void stop_postmaster_atexit(void)
```

## Detailed Description
This function serves as an atexit handler that ensures proper cleanup of the postmaster process during pg_upgrade operations. It's a simple wrapper around the stop_postmaster function that guarantees the postmaster will be terminated even if pg_upgrade exits unexpectedly. The function is marked as static, indicating it's only used within the server.c module of pg_upgrade.

## Parameters / Member Variables
- This function takes no parameters (void)

## Dependencies
- Functions called/Symbols referenced:
  - [stop_postmaster](stop_postmaster.md)
- Called from (representative examples):
  - Registered as atexit handler in start_postmaster

## Notes and Other Information
- This function is part of pg_upgrade's server management system
- It ensures cleanup happens automatically when the process exits
- The function calls stop_postmaster with the 'fast' parameter set to true
- Located in src/bin/pg_upgrade/server.c:191-197