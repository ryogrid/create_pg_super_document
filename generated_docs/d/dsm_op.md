# dsm_op

## Location
[src/include/storage/dsm_impl.h:67-79](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/dsm_impl.h#L67-L79)

## Overview
An enumeration type that defines the four basic operations that can be performed on dynamic shared memory segments: create, attach, detach, and destroy.

## Definition


## Detailed Description
The  enum serves as a command parameter for the dynamic shared memory implementation layer, specifically used by the  function to determine which operation to perform on a shared memory segment. This enumeration provides a type-safe way to specify the intended operation across different platform-specific implementations (POSIX, System V, Windows, and memory-mapped files).

The enum is part of PostgreSQL's dynamic shared memory subsystem, which provides a portable abstraction layer over various operating system shared memory facilities. Each enum value corresponds to a specific lifecycle operation on shared memory segments.

## Parameters / Member Variables
- : Create a new shared memory segment with the specified size
- : Attach to an existing shared memory segment and map it into the process address space
- : Detach from a shared memory segment by unmapping it from the process address space
- : Destroy a shared memory segment, first detaching if necessary, then removing it from the system

## Dependencies
- Functions called/Symbols referenced:
  - Used as parameter type in 
  - Used in platform-specific implementations: , , , 
- Called from (representative examples):
  -  (src/backend/storage/ipc/dsm_impl.c:159)
  - Platform-specific DSM implementation functions

## Notes and Other Information
- The enum values are used extensively in conditional logic within the DSM implementation to branch on the specific operation being performed
-  operations may silently fail with name collisions (return false without logging)
- Only  operations should specify a non-zero request_size parameter
-  and  operations handle cleanup of mapped memory segments
- The enum provides a consistent interface across multiple platform-specific shared memory implementations
- Part of the low-level DSM implementation layer defined in 