# expand_dbname_patterns

## Location
[src/bin/pg_dump/pg_dumpall.c:1528-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1528-L1580)

## Overview
Finds a list of database names that match the given patterns by querying the PostgreSQL system catalog and expanding pattern-based database name specifications.

## Definition

```c
static void
expand_dbname_patterns(PGconn *conn,
					   SimpleStringList *patterns,
					   SimpleStringList *names)
```
## Detailed Description
This function processes a list of database name patterns and expands them into actual database names by querying the  system catalog. It's designed for use in pg_dumpall to allow users to specify database selection patterns rather than explicit database names. The function is similar in concept to  in pg_dump.c.

For each pattern provided, the function constructs and executes a SQL query that uses PostgreSQL's pattern matching capabilities. The function validates that database name patterns don't contain improper qualified names (dotted names) since database names should be simple identifiers. All matching database names are appended to the output list, with duplicate entries being acceptable since the list is only used for membership testing.

## Parameters / Member Variables
- `*conn`: Active PostgreSQL database connection used to execute queries
- `*patterns`: Input list containing database name patterns to expand
- `*names`: Output list where matching database names will be appended
## Dependencies
- Functions called/Symbols referenced:
  - [SimpleStringList](../S/SimpleStringList.md) (data structure)
  - [SimpleStringListCell](../S/SimpleStringListCell.md) (iterator structure)  
  - processSQL PatternPattern (pattern matching utility)
  - [PQfinish](../P/PQfinish.md) (PostgreSQL connection cleanup)
  - [exit_nicely](exit_nicely.md) (graceful exit function)
  - [executeQuery](executeQuery.md) (query execution wrapper)
  - [simple_string_list_append](../s/simple_string_list_append.md) (list manipulation)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (buffer reset utility)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_dumpall.c at line 515)

## Notes and Other Information
- The function returns early if no patterns are provided (patterns->head == NULL)
- Multiple SELECT queries may result in duplicate entries in the output list, but this is intentional and acceptable
- Database name patterns containing dots (qualified names) are rejected as improper since database names should be simple identifiers
- Part of the pg_dumpall utility's pattern-based database selection mechanism
- Uses PostgreSQL's standard pattern matching syntax for database name expansion