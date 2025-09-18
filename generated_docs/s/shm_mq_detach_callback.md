# shm_mq_detach_callback

## Location
src/backend/storage/ipc/shm_mq.c: 1323 - 1328

## Overview
A static callback function that serves as a shim for dynamic shared memory (DSM) segment detachment, automatically detaching a shared message queue when its associated DSM segment is detached.

## Definition


## Detailed Description
The  function is a callback wrapper designed to be registered with the DSM subsystem via . When a DSM segment is being detached (either explicitly or during process cleanup), this callback ensures that any shared message queues associated with that segment are properly detached as well.

This function acts as a thin wrapper around , extracting the shared message queue pointer from the Datum argument and performing the actual detachment logic. The callback mechanism provides automatic cleanup when DSM segments are detached, preventing processes from blocking indefinitely when their communication counterparts disappear.

The function is critical for maintaining system reliability in PostgreSQL's inter-process communication infrastructure, particularly in scenarios involving background workers and parallel query execution.

## Parameters / Member Variables
- : Pointer to the DSM segment being detached (not directly used by this function)
- : A Datum containing a pointer to the  structure that should be detached

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts the shm_mq pointer from the Datum argument
  -  - Performs the actual message queue detachment logic
- Called from (representative examples):
  - DSM detachment callbacks registered via  in 
  - Automatic invocation during DSM segment cleanup

## Notes and Other Information
- This is a static function internal to src/backend/storage/ipc/shm_mq.c
- The function signature matches the  typedef required by the DSM subsystem
- Registered automatically when  is called with a non-NULL DSM segment
- Provides a layer of indirection between the DSM callback system and the shared message queue implementation
- Essential for preventing deadlocks and ensuring proper cleanup in multi-process scenarios
- The callback is invoked during both explicit DSM detachment and process termination cleanup