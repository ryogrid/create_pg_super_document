# PGSemaphore

## Location
src/include/storage/pg_sema.h: 34 - 36

## Overview
PGSemaphore is a platform-independent counting semaphore type that provides a unified API for semaphore operations across different operating systems in PostgreSQL.

## Definition


## Detailed Description
PGSemaphore is a typedef that abstracts platform-specific semaphore implementations to provide a consistent interface for PostgreSQL's synchronization needs. PostgreSQL requires counting semaphores that keep track of multiple unlock operations and allow an equal number of subsequent lock operations before blocking.

The implementation varies by platform:
- **Unix/POSIX systems**: PGSemaphore is a pointer to PGSemaphoreData structure containing a sem_t with padding
- **System V**: PGSemaphore points to PGSemaphoreData with semId and semNum fields for semaphore set management  
- **Windows**: PGSemaphore is directly defined as a HANDLE type for native Windows semaphore objects

The opaque PGSemaphoreData structure contents are platform-specific and should never be accessed directly by platform-independent code, ensuring portability across different operating systems.

## Parameters / Member Variables
Since PGSemaphore is a typedef to different types on different platforms:

**POSIX Implementation (PGSemaphoreData structure members):**
- : A SemTPadded structure containing the actual sem_t semaphore with appropriate padding

**System V Implementation (PGSemaphoreData structure members):**
- : Semaphore set identifier for the System V semaphore
- : Semaphore number within the semaphore set

**Windows Implementation:**
- Direct HANDLE type for Windows semaphore objects

## Dependencies
- Functions called/Symbols referenced:
  - PGSemaphoreData (structure definition varies by platform)
  - HANDLE (Windows platform only)

- Called from (representative examples):
  - PGSemaphoreCreate (creates new semaphore instances)
  - PGSemaphoreLock (locks/decrements semaphore)
  - PGSemaphoreUnlock (unlocks/increments semaphore)
  - PGSemaphoreTryLock (non-blocking lock attempt)
  - PGSemaphoreReset (resets semaphore to count 0)
  - SpinlockSemaInit (spin lock initialization)
  - PGPROC (process structure for semaphore management)

## Notes and Other Information
- The semaphore API is designed to be counting semaphores, not binary semaphores
- Platform-specific implementations handle the underlying OS differences transparently
- Windows implementation uses native HANDLE type for better performance and ABI compatibility
- All semaphore operations are thread-safe and can be used for inter-process synchronization
- The semaphore system is initialized during postmaster start or shared memory reinitialization
- Memory for semaphores is allocated from PostgreSQL's shared memory segment
- Each semaphore starts with an initial count of 1 when created via PGSemaphoreCreate
- Location: src/include/storage/pg_sema.h:34-36