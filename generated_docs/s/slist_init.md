# slist_init

## Location
[src/include/lib/ilist.h:986-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/ilist.h#L986-L994)

## Overview
Initializes a singly-linked list by setting the head node to an empty state, discarding any previous contents without cleanup.

## Definition
```c
static inline void
slist_init(slist_head *head)
```

## Detailed Description
This function initializes or reinitializes a singly-linked list by setting the head node's next pointer to NULL, effectively creating an empty list. The function is designed to be simple and efficient, but it does not perform any cleanup of existing list contents - any previous state is simply discarded. This makes it suitable for initializing new lists or resetting lists where cleanup has already been handled separately.

## Parameters / Member Variables
- `head`: Pointer to the singly-linked list head structure to initialize

## Dependencies
- Functions called/Symbols referenced:
  - [slist_head](slist_head.md) (parameter type)
- Called from (representative examples):
  - [EventTriggerBeginCompleteQuery](../E/EventTriggerBeginCompleteQuery.md) (event trigger processing)
  - SPI_connect_ext (Server Programming Interface)
  - [dsm_create_descriptor](../d/dsm_create_descriptor.md) (dynamic shared memory)
  - [InitCatCache](../I/InitCatCache.md) (system catalog cache)

## Notes and Other Information
- This is a static inline function for optimal performance
- Does not perform any cleanup of existing list contents - previous state is discarded
- Sets head->head.next to NULL to create an empty list
- Part of PostgreSQL's intrusive singly-linked list implementation
- Should be used when initializing a new list or when existing contents have already been properly cleaned up
- Commonly used during system initialization and setup phases