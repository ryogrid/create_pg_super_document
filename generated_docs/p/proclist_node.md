# proclist_node

## Location
[src/include/storage/proclist_types.h:28-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/proclist_types.h#L28-L32)

## Overview
A structure representing a node in a doubly-linked list of PostgreSQL processes, using 0-based PGPROC indexes for linking rather than memory pointers.

## Definition

```c
typedef struct proclist_node
{
	ProcNumber	next;			/* pgprocno of the next PGPROC */
	ProcNumber	prev;			/* pgprocno of the prev PGPROC */
} proclist_node;
```
## Detailed Description
The  structure implements a doubly-linked list node for managing PostgreSQL processes. Instead of using traditional memory pointers, it uses  values (0-based PGPROC indexes) to reference the next and previous nodes in the list. This approach provides several advantages:

1. **Process Index-Based Linking**: Uses  indexes instead of pointers, making the structure more suitable for shared memory environments where pointer addresses may vary across processes.
2. **Boundary Conditions**: Uses  in the next-link of the last node and prev-link of the first node to mark list boundaries.
3. **Unlinked State**: A node not currently in any list has both , which is distinguishable from valid list states since circularity is explicitly disallowed.

## Parameters / Member Variables
- : ProcNumber (0-based PGPROC index) of the next process in the list, or  if this is the last node
- : ProcNumber (0-based PGPROC index) of the previous process in the list, or  if this is the first node

## Dependencies
- Functions called/Symbols referenced:
  - ProcNumber (typedef used for next/prev fields)
- Called from (representative examples):
  - PGPROC (contains proclist_node members)
  - proclist_is_empty
  - proclist_node_get
  - proclist_push_head_offset
  - proclist_push_tail_offset
  - proclist_delete_offset
  - proclist_contains_offset

## Notes and Other Information
- This structure is designed for use in shared memory contexts where traditional pointer-based linked lists would be problematic
- The use of process numbers instead of pointers allows the same data structure to be safely accessed from different processes
- A node with next == prev == 0 is guaranteed to not be in any list, providing a clear unlinked state
- The design explicitly prevents circular lists by using this special unlinked state convention