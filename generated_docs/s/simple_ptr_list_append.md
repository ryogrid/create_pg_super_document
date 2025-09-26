# simple_ptr_list_append

## Location
[src/fe_utils/simple_list.c:162-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/simple_list.c#L162-L175)

## Overview
Appends a generic pointer to a SimplePtrList, maintaining the linked list structure with proper tail pointer management.

## Definition
void simple_ptr_list_append(SimplePtrList *list, void *ptr)

## Detailed Description
This function adds a new pointer to the end of a SimplePtrList by creating a new SimplePtrListCell and linking it appropriately. The function allocates memory for a new cell, stores the provided pointer, and updates the list's head and tail pointers to maintain proper linked list structure. If the list is empty (tail is NULL), the new cell becomes both the head and tail. Otherwise, the new cell is linked after the current tail and becomes the new tail.

## Parameters / Member Variables
- : Pointer to the SimplePtrList structure to append to
- : Generic void pointer to be stored in the list (caller must ensure pointer remains valid)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (PostgreSQL memory allocation function)
- Data structures used:
  - [SimplePtrList](../S/SimplePtrList.md)
  - [SimplePtrListCell](../S/SimplePtrListCell.md)
- Called from (representative examples):
  - [compile_database_list](../c/compile_database_list.md) (src/bin/pg_amcheck/pg_amcheck.c:1601, 1741)
  - [compile_relation_list_one_db](../c/compile_relation_list_one_db.md) (src/bin/pg_amcheck/pg_amcheck.c:2219)
  - [flagInhIndexes](../f/flagInhIndexes.md) (src/bin/pg_dump/common.c:470)

## Notes and Other Information
- The caller is responsible for ensuring that the appended pointer remains valid for the lifetime of the list
- Memory for the new cell is allocated using pg_malloc, which will exit on allocation failure
- The function maintains both head and tail pointers for efficient O(1) append operations
- Used primarily in PostgreSQL frontend utilities for collecting lists of database objects and relations
- Located in src/fe_utils/simple_list.c:162-175