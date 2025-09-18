# ThereIsAtLeastOneRole

## Location
src/backend/utils/init/postinit.c: 1453 - 1468

## Overview
A utility function that checks whether at least one role (user or role) is defined in the database cluster by scanning the pg_authid system catalog.

## Definition
```c
static bool ThereIsAtLeastOneRole(void)
```

## Detailed Description
This function performs a catalog scan on the pg_authid system table to determine if any roles exist in the database cluster. It opens the pg_authid relation with an AccessShareLock, initiates a catalog scan, and attempts to retrieve the first tuple. If any tuple is found, it indicates that at least one role exists in the system. The function is used during database initialization to verify that the cluster has been properly bootstrapped with initial roles.

The function uses PostgreSQL's table access methods to safely scan the system catalog and properly handles locking and cleanup of resources.

## Parameters / Member Variables
- None (void function)
- Returns:  - true if at least one role exists, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - TableScanDesc (type)
  - table_open
  - table_beginscan_catalog
  - heap_getnext
  - ForwardScanDirection (constant)
  - table_endscan
  - table_close
- Called from (representative examples):
  - InitPostgres

## Notes and Other Information
- This is a static function within postinit.c, limiting its visibility to that compilation unit
- Uses AccessShareLock to ensure safe concurrent access to the pg_authid catalog
- Part of the database initialization process to verify proper cluster setup
- Performs minimal scanning - stops after finding the first role rather than counting all roles
- Properly handles resource cleanup by closing the scan and relation even if no roles are found
- Critical for detecting improperly initialized database clusters that lack essential bootstrap roles