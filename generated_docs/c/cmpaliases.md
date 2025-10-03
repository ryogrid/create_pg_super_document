# cmpaliases

## Location
[src/backend/commands/collationcmds.c:631-649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L631-L649)

## Overview
A comparison function used as a qsort comparator for CollAliasData items, enabling sorting of locale alias data by locale name.

## Definition

```c
static int
cmpaliases(const void *a, const void *b)
```
## Detailed Description
The cmpaliases function serves as a comparison callback for the qsort() library function to sort arrays of CollAliasData structures. It performs a lexicographic comparison of the localename field between two CollAliasData items, which is sufficient for ordering because other fields (alias and encoding) are derived from the locale name. This function is essential for organizing locale data in a predictable order during collation import operations.

## Parameters / Member Variables
- `*a`: Pointer to the first CollAliasData item to compare (cast from void*)
- `*b`: Pointer to the second CollAliasData item to compare (cast from void*)
## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function)
  - CollAliasData (struct type)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md) (via qsort callback)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only visible within collationcmds.c
- The function follows the standard qsort comparator contract: returns negative if a < b, zero if a == b, positive if a > b
- Only compares the localename field as the comment indicates other fields are derived from it
- Part of PostgreSQL's collation management system for importing system locale information