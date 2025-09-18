# get_ENR

## Location
[src/backend/utils/misc/queryenvironment.c:96-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/queryenvironment.c#L96-L124)

## Overview
Searches for and returns an ephemeral named relation (ENR) by name from a query environment's collection of named relations.

## Definition
```c
EphemeralNamedRelation get_ENR(QueryEnvironment *queryEnv, const char *name)
```

## Detailed Description
This function performs a linear search through the namedRelList of a query environment to find an ephemeral named relation with a matching name. It uses string comparison (strcmp) to match the provided name against the name field in each ENR's metadata. The function is designed to quietly return NULL if no match is found or if the query environment itself is NULL. This is a fundamental lookup function used throughout the ENR system.

## Parameters / Member Variables
- `queryEnv`: The QueryEnvironment to search in (can be NULL)
- `name`: The name of the ephemeral named relation to find (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - foreach (PostgreSQL list iteration macro)
  - lfirst (PostgreSQL list cell accessor)
  - strcmp (standard C string comparison)
  - Assert (for parameter validation)
- Called from (representative examples):
  - [ExecInitNamedTuplestoreScan](../E/ExecInitNamedTuplestoreScan.md)
  - [_SPI_find_ENR_by_name](../S/_SPI_find_ENR_by_name.md)
  - [get_visible_ENR_metadata](get_visible_ENR_metadata.md)
  - [register_ENR](../r/register_ENR.md)
  - [unregister_ENR](../u/unregister_ENR.md)

## Notes and Other Information
- Returns NULL quietly if no match is found (no error is raised)
- Returns NULL if queryEnv is NULL
- Includes an assertion that name must not be NULL
- Performs linear search through the named relation list
- This is a read-only operation that doesn't modify the query environment
- The function is used internally by other ENR management functions