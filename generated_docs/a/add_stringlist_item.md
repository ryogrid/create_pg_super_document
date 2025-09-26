# add_stringlist_item

## Location
[src/bin/initdb/initdb.c:442-469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L442-L469)

## Overview
Adds a new string item to the end of a linked list of strings, managing memory allocation and list traversal automatically.

## Definition
```c
static void add_stringlist_item(_stringlist **listhead, const char *str)
```

## Detailed Description
This function implements a simple linked list append operation for string lists. It creates a new _stringlist node, duplicates the provided string into it, and adds it to the end of the existing list. If the list is empty (listhead points to NULL), the new item becomes the first and only item. Otherwise, the function traverses to the end of the list and links the new item there. The function handles all memory allocation automatically, creating both the list node structure and a copy of the string data. This is commonly used throughout PostgreSQL tools for building lists of configuration options, test names, and other string collections.

## Parameters / Member Variables
- `listhead`: Pointer to the head pointer of the string list (allows modification of the list head)
- `str`: The string to be added to the list (will be duplicated, not referenced)

## Dependencies
- Functions called/Symbols referenced:
  - [_stringlist](../s/_stringlist.md) (structure type for list nodes)
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation)
  - [pg_strdup](../p/pg_strdup.md) (PostgreSQL string duplication)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/initdb/initdb.c:3262, 3263)
  - [ecpg_start_test](../e/ecpg_start_test.md) (in src/interfaces/ecpg/test/pg_regress_ecpg.c:198-208)
  - [isolation_start_test](../i/isolation_start_test.md) (in src/test/isolation/isolation_main.c:75, 76)
  - [isolation_init](../i/isolation_init.md) (in src/test/isolation/isolation_main.c:133)
  - [split_to_stringlist](../s/split_to_stringlist.md) (in src/test/regress/pg_regress.c:241)
  - [regression_main](../r/regression_main.md) (in src/test/regress/pg_regress.c:2170, 2202, 2211, 2235)
  - [psql_start_test](../p/psql_start_test.md) (in src/test/regress/pg_regress_main.c:62, 63)
  - [psql_init](../p/psql_init.md) (in src/test/regress/pg_regress_main.c:107)

## Notes and Other Information
- Creates a copy of the input string, so the original can be safely freed
- Always appends to the end of the list, maintaining insertion order
- Handles empty list initialization automatically
- Used extensively in PostgreSQL testing infrastructure and initdb
- Memory for both the list node and string copy is allocated and managed by the function
- The list head pointer may be modified if the list was initially empty