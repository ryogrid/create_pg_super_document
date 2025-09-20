# InitQueryHashTable

## Location
[src/backend/commands/prepare.c:369-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/prepare.c#L369-L388)

## Overview
Initializes the global hash table for storing prepared statements, setting up the data structure used to manage named prepared statements by their statement names.

## Definition

```c
static void
InitQueryHashTable(void)
```
## Detailed Description
InitQueryHashTable creates and configures the global hash table that stores prepared statements indexed by their names. The function sets up a hash table with string keys (statement names) that can store PreparedStatement structures. It configures the hash table to use PostgreSQL's standard string hashing functions and sets appropriate size parameters for efficient prepared statement lookup and storage.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md) (creates new hash table)
  - NAMEDATALEN (defines maximum name length for keys)
  - PreparedStatement (defines entry size for hash table)
  - HASH_ELEM (hash table creation flag for element management)
  - HASH_STRINGS (hash table creation flag for string key handling)
- Called from (representative examples):
  - [StorePreparedStatement](../S/StorePreparedStatement.md) (initializes hash table on first prepared statement storage)

## Notes and Other Information
- Creates a hash table named "Prepared Queries" for debugging and monitoring purposes
- Uses NAMEDATALEN as key size to match PostgreSQL's standard name length limits
- Initial hash table size is set to 32 entries with automatic expansion as needed
- Uses HASH_ELEM flag to enable proper hash table element management
- Uses HASH_STRINGS flag to enable string-based key comparison and hashing
- Called lazily only when the first prepared statement needs to be stored
- The global prepared_queries variable stores the hash table reference for subsequent operations
- Part of PostgreSQL's prepared statement infrastructure for efficient statement reuse