# ecpg_sqlca_key_destructor

## Location
src/interfaces/ecpg/ecpglib/misc.c: 96 - 101

## Overview
A pthread cleanup function that frees the memory allocated for thread-local SQLCA structures when a thread terminates.

## Definition


## Detailed Description
The  function serves as a destructor callback for pthread-specific data associated with SQLCA structures. This function is automatically called by the pthread library when a thread terminates, ensuring that any dynamically allocated SQLCA structure associated with that thread is properly freed to prevent memory leaks.

The function is designed to work with the pthread key-value system, where each thread can have its own instance of a SQLCA structure stored as thread-local data. When the thread exits, this destructor ensures cleanup of the allocated memory.

## Parameters / Member Variables
- : Pointer to the SQLCA structure that was allocated for the terminating thread. This is the same pointer that was stored as thread-specific data and needs to be freed.

## Dependencies
- Functions called/Symbols referenced:
  - free (standard library function for memory deallocation)
- Called from (representative examples):
  - ecpg_sqlca_key_init (registered as destructor callback)
  - pthread library (automatically called on thread termination)

## Notes and Other Information
- Static function, only visible within the misc.c compilation unit
- Critical for preventing memory leaks in multi-threaded applications using ECPG
- Works in conjunction with pthread_key_create and the thread-local storage system
- The SQLCA structures are allocated in ECPGget_sqlca() function
- Part of the thread-safe SQLCA management system in ECPG
- Automatically invoked by pthread library cleanup mechanism