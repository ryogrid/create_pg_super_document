# proclist_head

## Location
[src/include/storage/proclist_types.h:38-42](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist_types.h#L38-L42)

## Overview
A structure representing the header/control block for a doubly-linked list of PostgreSQL processes, containing pointers to the first and last processes in the list.

## Definition

```c
typedef struct proclist_head
{
	ProcNumber	head;			/* pgprocno of the head PGPROC */
	ProcNumber	tail;			/* pgprocno of the tail PGPROC */
} proclist_head;
```
## Detailed Description
The `proclist_head` structure serves as the control header for a doubly-linked list of PostgreSQL processes. It maintains references to both ends of the list using `ProcNumber` indexes rather than memory pointers, making it suitable for shared memory environments.

Key characteristics:
1. **Dual-End Access**: Maintains both head and tail references for O(1) insertion/removal at either end
2. **Empty List Representation**: An empty list is indicated by both head and tail being set to `INVALID_PROC_NUMBER`
3. **Process Index-Based**: Uses `ProcNumber` values instead of pointers for cross-process compatibility
4. **Efficient Operations**: Enables efficient queue and stack operations on process lists

## Parameters / Member Variables
- `head`: ProcNumber (0-based PGPROC index) of the first process in the list, or `INVALID_PROC_NUMBER` if the list is empty
- `tail`: ProcNumber (0-based PGPROC index) of the last process in the list, or `INVALID_PROC_NUMBER` if the list is empty

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber (typedef used for head/tail fields)
- Called from (representative examples):
  - [LWLockWakeup](../L/LWLockWakeup.md)
  - [LWLockUpdateVar](../L/LWLockUpdateVar.md)  
  - CONDITION_VARIABLE_H
  - [LWLock](../L/LWLock.md)
  - [proclist_init](proclist_init.md)
  - [proclist_is_empty](proclist_is_empty.md)
  - [proclist_push_head_offset](proclist_push_head_offset.md)
  - [proclist_push_tail_offset](proclist_push_tail_offset.md)
  - [proclist_delete_offset](proclist_delete_offset.md)
  - [proclist_contains_offset](proclist_contains_offset.md)
  - [proclist_pop_head_node_offset](proclist_pop_head_node_offset.md)
  - proclist_foreach_modify

## Notes and Other Information
- Designed for use in shared memory where traditional pointer-based data structures would be problematic
- The head/tail design allows for efficient implementation of both queue (FIFO) and stack (LIFO) operations
- Empty list state (head == tail == INVALID_PROC_NUMBER) is clearly distinguishable from single-element lists
- Used extensively in PostgreSQL's locking and synchronization mechanisms
- Works in conjunction with `proclist_node` structures embedded in PGPROC entries to form complete linked lists