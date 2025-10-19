# check_new_cluster_is_empty

## Location
[src/bin/pg_upgrade/check.c:853-884](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L853-L884)

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

## Dependencies
- Functions called/Symbols referenced:
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
  - strcmp (for string comparison)
  - RelInfoArr (relation information array structure)
- Called from (representative examples):
  - [check_new_cluster](check_new_cluster.md) (main cluster validation function)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- The function specifically skips pg_largeobject and its index as noted in the comment
- Failure results in immediate program termination via pg_fatal
- This validation is essential for preventing data corruption during cluster upgrades
- The function assumes that new_cluster global variable is properly initialized

## Simplified Source

```c
static void check_new_cluster_is_empty(void) {
    // Check each database in the new cluster
    for (int dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++) {
        RelInfoArr *rel_arr = &new_cluster.dbarr.dbs[dbnum].rel_arr;

        // Check each relation in the database
        for (int relnum = 0; relnum < rel_arr->nrels; relnum++) {
            // Only pg_catalog relations are allowed (system catalog)
            if (strcmp(rel_arr->rels[relnum].nspname, "pg_catalog") != 0) {
                pg_fatal("New cluster database \"%s\" is not empty: found relation \"%s.%s\"",
                         new_cluster.dbarr.dbs[dbnum].db_name,
                         rel_arr->rels[relnum].nspname,
                         rel_arr->rels[relnum].relname);
            }
        }
    }
}
```