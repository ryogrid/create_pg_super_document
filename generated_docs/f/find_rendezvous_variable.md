# find_rendezvous_variable

## Location
[src/backend/utils/fmgr/dfmgr.c:599-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/dfmgr.c#L599-L636)

## Overview
Creates or retrieves a shared rendezvous variable that allows dynamically loaded libraries to communicate and share data.

## Definition


## Detailed Description
This function implements a mechanism for inter-library communication within a PostgreSQL process. It provides a way for different dynamically loaded libraries to establish shared variables for data exchange, coordination, or other communication needs.

The function maintains a process-wide hash table of rendezvous variables, where each variable is identified by a string name and contains a void pointer value. The workflow is:

1. **Hash Table Initialization**: On first call, creates a static hash table using PostgreSQL's hash table infrastructure with  as key size and  as entry structure
2. **Variable Lookup/Creation**: Searches for an existing variable with the given name using  with  flag, which creates the entry if it doesn't exist
3. **Initialization**: If this is the first time the variable is accessed, initializes its value to NULL
4. **Return Address**: Returns the address of the variable's value pointer, allowing callers to read or modify the shared data

The rendezvous variables persist for the entire lifetime of the process, enabling long-term communication between libraries.

## Parameters / Member Variables
- : The name identifier for the rendezvous variable, used as the hash table key

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL hash table type
  -  - [hash](../h/hash.md) table entry structure containing the variable value
  -  - [hash](../h/hash.md) table control structure for configuration
  -  - PostgreSQL standard name length constant
  -  - creates a new hash table
  -  - finds or creates hash table entries
  -  - [hash](../h/hash.md) table flag for element-based operations
  -  - [hash](../h/hash.md) table flag for string key operations
  -  - [hash](../h/hash.md) table operation flag for insert-or-find
- Called from:
  -  (src/include/fmgr.h:748)
  -  (src/pl/plpython/plpy_main.c:74)

## Notes and Other Information
- Uses a static local hash table that persists across function calls within the same process
- The returned pointer points to the variable's value, not the variable itself - callers can dereference it to access/modify the shared data
- Rendezvous variables are particularly useful for PostgreSQL extensions that need to share state or coordinate behavior
- The hash table is created with an initial size of 16 entries and uses string-based hashing
- Thread safety depends on PostgreSQL's overall threading model since this uses static storage
- Part of PostgreSQL's dynamic function manager (dfmgr) infrastructure for supporting complex extension interactions