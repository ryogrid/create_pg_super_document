# check_new_cluster_is_empty

## Location
src/bin/pg_upgrade/check.c: 853 - 884

## Overview
This function validates that the new PostgreSQL cluster is empty before performing an upgrade, ensuring that user-created relations do not exist in non-system schemas.

## Definition
```c
static void check_new_cluster_is_empty(void)
```

## Detailed Description
The `check_new_cluster_is_empty` function performs a critical validation step during PostgreSQL cluster upgrades by examining all databases in the new cluster to ensure they contain only system catalog relations. It iterates through each database in the new cluster and checks every relation to verify that only relations in the `pg_catalog` namespace exist. This validation prevents upgrade conflicts that could occur if user data already exists in the target cluster.

The function operates by:
1. Iterating through all databases in the `new_cluster.dbarr` array
2. For each database, examining all relations in its relation array
3. Checking each relation's namespace name
4. Terminating the upgrade process with a fatal error if any non-pg_catalog relations are found

## Parameters / Member Variables
This function takes no parameters and operates on global cluster state.

## Dependencies
- Functions called/Symbols referenced:
  - pg_fatal (for error reporting)
  - strcmp (for string comparison)
  - RelInfoArr (relation information array structure)
- Called from (representative examples):
  - check_new_cluster (main cluster validation function)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- The function specifically skips pg_largeobject and its index as noted in the comment
- Failure results in immediate program termination via pg_fatal
- This validation is essential for preventing data corruption during cluster upgrades
- The function assumes that new_cluster global variable is properly initialized