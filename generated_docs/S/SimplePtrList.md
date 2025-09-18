# SimplePtrList

## Location
[src/include/fe_utils/simple_list.h:52-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/simple_list.h#L52-L56)

## Overview
SimplePtrList is a simple linked list data structure for frontend code that stores void pointers, providing basic list functionality for PostgreSQL client utilities.

## Definition


## Detailed Description
SimplePtrList is part of PostgreSQL's frontend utility simple list facilities designed for client-side code such as pg_dump and pg_amcheck. It provides a lightweight alternative to the backend's more sophisticated List data structure when only basic list operations are needed. The list maintains pointers to arbitrary data through void pointers, allowing storage of references to various data types without type-specific implementations.

The structure implements a singly-linked list with head and tail pointers for efficient append operations. Unlike the backend List facilities, this implementation is intentionally minimal and primitive, providing just the essential functionality needed by frontend utilities.

## Parameters / Member Variables
- : Pointer to the first SimplePtrListCell in the list, or NULL if the list is empty
- : Pointer to the last SimplePtrListCell in the list, or NULL if the list is empty; used for efficient O(1) append operations

## Dependencies
- Functions called/Symbols referenced:
  - [SimplePtrListCell](SimplePtrListCell.md)
- Called from (representative examples):
  - [main](../m/main.md) (pg_amcheck.c:223, 224)
  - [compile_database_list](../c/compile_database_list.md) (pg_amcheck.c:1583)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (pg_amcheck.c:1883)
  - [getIndexes](../g/getIndexes.md) (pg_dump.c:7683)
  - [simple_ptr_list_append](../s/simple_ptr_list_append.md) (simple_list.c:162)

## Notes and Other Information
- Part of a family of simple list types including SimpleOidList and SimpleStringList
- Designed specifically for frontend utilities where the backend's List infrastructure is not available
- Only one manipulation function is provided: simple_ptr_list_append()
- No destroy function is provided for SimplePtrList, unlike SimpleOidList and SimpleStringList
- Callers are responsible for ensuring that stored pointers remain valid throughout the list's lifetime
- The list structure should be initialized with head and tail set to NULL before first use
- Located in src/include/fe_utils/simple_list.h:52-56