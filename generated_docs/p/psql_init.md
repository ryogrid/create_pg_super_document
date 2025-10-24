# psql_init

## Location
[src/test/regress/pg_regress_main.c:104-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress_main.c#L104-L110)

## Overview
Initializes the PostgreSQL regression test framework by setting up the default database configuration for test execution.

## Definition

```c
static void
psql_init(int argc, char **argv)
```
## Detailed Description
This function performs the initialization setup for the PostgreSQL regression testing framework. It sets up the default configuration by adding the "regression" database to the database list that will be used for running tests. This is a simple but essential initialization step that ensures tests have a default target database when no specific database is specified through command-line arguments.

## Parameters / Member Variables
- `argc`: Number of command-line arguments (currently unused)
- `**argv`: Array of command-line argument strings (currently unused)
## Dependencies
- Functions called/Symbols referenced:
  - [add_stringlist_item](../a/add_stringlist_item.md): Adds the "regression" database to the global dblist
- Called from (representative examples):
  - [main](../m/main.md) (in src/test/regress/pg_regress_main.c:114)

## Notes and Other Information
- Currently does not process command-line arguments, but the parameters are provided for potential future extension
- Works with the global dblist variable to maintain the list of databases for testing
- The "regression" database is the standard default database used by PostgreSQL regression tests
- This function is part of the initialization sequence in the regression test framework

## Simplified Source

```c
static void
psql_init(int argc, char **argv)
{
    /* set default regression database name */
    add_stringlist_item(&dblist, "regression");
}
```