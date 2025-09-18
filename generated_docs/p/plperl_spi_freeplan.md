# plperl_spi_freeplan

## Location
[src/pl/plperl/plperl.c:3960-3990](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L3960-L3990)

## Overview
Releases a previously prepared SQL statement and all associated resources from the PL/Perl query cache.

## Definition
```c
void plperl_spi_freeplan(char *query)
```

## Detailed Description
This function deallocates a prepared SQL statement that was previously created and cached in the PL/Perl interpreter's query hash. It performs a complete cleanup of all resources associated with the prepared statement, including removing the entry from the hash table, deleting the memory context that contains the query descriptor and related data, and finally calling SPI_freeplan to release the PostgreSQL execution plan.

The function follows a careful cleanup order to ensure that if SPI_freeplan fails, no orphaned data structures remain. It first removes the hash entry and deletes the memory context before calling SPI_freeplan, ensuring that all PL/Perl-managed resources are cleaned up even if the SPI cleanup fails.

## Parameters / Member Variables
- `query`: String identifier for the prepared statement to deallocate

## Dependencies
- Functions called/Symbols referenced:
  - [check_spi_usage_allowed](../c/check_spi_usage_allowed.md)
  - [hash_search](../h/hash_search.md) (with HASH_FIND to locate, HASH_REMOVE to delete)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (cleanup query descriptor memory)
  - [SPI_freeplan](../S/SPI_freeplan.md) (release PostgreSQL execution plan)
- Called from (representative examples):
  - PL_PERL_H (header declaration)

## Notes and Other Information
- Performs validation to ensure the query identifier exists in the hash table
- Uses a defensive cleanup strategy: removes PL/Perl data structures before calling SPI_freeplan
- The memory context deletion automatically frees the query descriptor and associated metadata
- No sub-transaction is needed since this is a cleanup operation that should not fail
- Essential for preventing memory leaks in long-running PL/Perl functions that prepare many statements
- Should be called when prepared statements are no longer needed to free up resources