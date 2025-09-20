# ONEXIT

## Location
[src/backend/storage/ipc/ipc.c:73-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L73-L103)

## Overview
ONEXIT is a structure that represents a single callback entry in PostgreSQL's process exit management system, storing a function pointer and its associated argument for deferred execution during process termination.

## Definition

```c
struct ONEXIT
{
	pg_on_exit_callback function;
	Datum		arg;
};
```
## Detailed Description
The ONEXIT struct is a fundamental component of PostgreSQL's Inter-Process Communication (IPC) exit handling mechanism. It serves as a container for callback functions that need to be executed during various stages of process termination. This structure enables PostgreSQL to maintain ordered lists of cleanup functions that are called at specific points during the shutdown sequence.

The structure is used to populate three distinct static arrays in the IPC subsystem:
- : Functions called during normal process exit
- : Functions called when exiting shared memory context  
- : Functions called before shared memory cleanup begins

Each array can hold up to MAX_ON_EXITS (20) callback entries, providing a robust cleanup mechanism for resource management during PostgreSQL backend termination.

## Parameters / Member Variables
- : A function pointer of type  that points to the cleanup function to be called. The callback signature is  where  is the exit code and  is the associated data.
- : A  value containing arbitrary data that will be passed to the callback function when it's invoked during exit processing.

## Dependencies
- Functions called/Symbols referenced:
  -  (typedef for function pointer type)
  -  (maximum number of exit callbacks)
  -  (PostgreSQL's generic data type)

- Called from (representative examples):
  -  (registers callbacks for process exit)
  -  (registers early cleanup callbacks)
  -  (registers shared memory exit callbacks)

## Notes and Other Information
- The ONEXIT structure is defined in  and is primarily used internally by the IPC exit management system
- The structure supports PostgreSQL's deterministic cleanup model, ensuring resources are freed in a predictable order during backend termination
- The  type for the arg member allows for flexible data passing, as it can hold various PostgreSQL data types including pointers, integers, and other values
- The limitation of MAX_ON_EXITS (20 callbacks) per exit type helps prevent excessive callback registration while providing sufficient flexibility for typical use cases
- This structure is critical for PostgreSQL's reliability, as improper cleanup during exit can lead to resource leaks, shared memory corruption, or other system-level issues