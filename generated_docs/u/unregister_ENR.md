# unregister_ENR

## Location
[src/backend/utils/misc/queryenvironment.c:82-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/queryenvironment.c#L82-L95)

## Overview
Removes an ephemeral named relation (ENR) from a query environment by name, making it no longer available for query processing.

## Definition
```c
void unregister_ENR(QueryEnvironment *queryEnv, const char *name)
```

## Detailed Description
This function removes an ephemeral named relation from a query environment's list of available named relations. It first searches for the ENR by name using get_ENR, and if found, removes it from the namedRelList using PostgreSQL's list_delete function. The function operates safely - if no ENR with the given name is found, no action is taken. According to the source comments, this is expected to be a rarely used function but is provided for completeness.

## Parameters / Member Variables
- `queryEnv`: The QueryEnvironment from which to remove the ENR
- `name`: The name of the ephemeral named relation to unregister

## Dependencies
- Functions called/Symbols referenced:
  - [get_ENR](../g/get_ENR.md) (to locate the ENR by name)
  - [list_delete](../l/list_delete.md) (PostgreSQL list manipulation function)
- Called from (representative examples):
  - [SPI_unregister_relation](../S/SPI_unregister_relation.md)

## Notes and Other Information
- The function operates safely - no error occurs if the named ENR doesn't exist
- This is a mutating operation that modifies the query environment
- According to PostgreSQL developers, this function is expected to be rarely used
- The function was provided "just in case" it might be needed
- Memory management of the removed ENR is handled by PostgreSQL's memory context system