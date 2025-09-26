# list_delete_last

## Location
[src/backend/nodes/list.c:957-982](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L957-L982)

## Overview
Removes and deallocates the last element from a PostgreSQL List, providing an efficient alternative to deleting from the front of the list.

## Definition

```c
List *
list_delete_last(List *list)
```
## Detailed Description
This function removes the last element from a PostgreSQL List data structure and returns the modified list. It is designed as a more efficient alternative to list_delete_first() when the application can work with elements in reverse order.

The function handles edge cases carefully: it returns NIL for empty lists, and when the list has only one element, it properly frees the entire list structure and returns NIL rather than leaving an empty list structure. For lists with multiple elements, it uses list_truncate() to remove the last element efficiently.

The efficiency advantage comes from the fact that removing the last element doesn't require shifting remaining elements, unlike removing from the front of the list.

## Parameters / Member Variables
- : The PostgreSQL List from which to remove the last element. Can be NIL (empty list).

## Dependencies
- Functions called/Symbols referenced:
  - check_list_invariants: Validates list structure integrity
  - list_length: Determines the number of elements in the list
  - list_free: Deallocates the entire list when it becomes empty
  - list_truncate: Removes elements from the end of the list
- Called from (representative examples):
  - CheckAttributeType: Type system validation
  - LockViewRecurse: View locking operations
  - agg_refill_hash_table: Aggregate function processing
  - inline_function: Function inlining optimization
  - transformOnConflictClause: UPSERT statement parsing

## Notes and Other Information
- Returns NIL if the input list is empty (no error is raised)
- More efficient than list_delete_first() as it doesn't require element shifting
- Properly handles the single-element case by freeing the list structure completely
- The comment in list_delete_first() suggests using this function when possible for better performance
- Uses list_truncate() internally for the actual removal operation
- Maintains list structure invariants through check_list_invariants()
- Commonly used in parsing and rewrite rule processing where LIFO (Last In, First Out) access patterns are beneficial