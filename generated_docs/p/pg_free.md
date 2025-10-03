# pg_free

## Location
[src/common/fe_memutils.c:105-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/fe_memutils.c#L105-L114)

## Overview
A simple wrapper around the standard C library free() function, providing a consistent interface for memory deallocation in PostgreSQL frontend utilities.

## Definition

```c
void
pg_free(void *ptr)
```
## Detailed Description
pg_free is a straightforward wrapper around the standard C library free() function. It serves as PostgreSQL's frontend counterpart to memory deallocation, providing a consistent API for freeing memory that was allocated using frontend memory utilities. The function simply passes the pointer directly to the standard free() function without any additional processing, validation, or error handling.

This function is part of PostgreSQL's frontend memory utilities located in src/common/fe_memutils.c, designed to provide a uniform memory management interface for client-side tools and utilities.

## Parameters / Member Variables
- `*ptr`: A pointer to the memory block to be freed. Can be NULL (free() handles NULL pointers safely by doing nothing).
## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)

- Called from (representative examples):
  - [icu_language_tag](../i/icu_language_tag.md) (src/bin/initdb/initdb.c:2336)
  - [setlocales](../s/setlocales.md) (src/bin/initdb/initdb.c:2478)
  - [main](../m/main.md) (src/bin/pg_basebackup/pg_basebackup.c:2597)
  - [get_standby_sysid](../g/get_standby_sysid.md) (src/bin/pg_basebackup/pg_createsubscriber.c:618)
  - [parseAclItem](parseAclItem.md) (src/bin/pg_dump/dumputils.c:439)
  - [pfree](pfree.md) (src/common/fe_memutils.c:135)
  - And many other frontend utilities across the PostgreSQL codebase

## Notes and Other Information
- This function is extensively used throughout PostgreSQL's frontend tools and utilities
- Unlike the backend's pfree() function which works with PostgreSQL's memory contexts, pg_free() works with standard malloc/free semantics
- The function provides no additional safety checks or error handling beyond what the standard free() provides
- It's safe to pass NULL to this function, as the underlying free() handles NULL pointers correctly
- This function is the standard way to free memory in PostgreSQL frontend code that was allocated with pg_malloc(), pg_malloc0(), or similar frontend allocation functions