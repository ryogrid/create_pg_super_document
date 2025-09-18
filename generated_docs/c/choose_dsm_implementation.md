# choose_dsm_implementation

## Location
src/bin/initdb/initdb.c: 1071 - 1112

## Overview
A static function in initdb that determines the most appropriate dynamic shared memory implementation for the target platform by testing POSIX shared memory availability and falling back to platform-specific alternatives.

## Definition


## Detailed Description
The  function performs runtime detection to select the optimal dynamic shared memory (DSM) implementation for PostgreSQL on the current platform. It prioritizes POSIX shared memory () because it typically offers higher default allocation limits compared to System V shared memory. However, since the mere presence of  doesn't guarantee successful operation, the function performs an empirical test by attempting to create and immediately destroy a test shared memory segment, similar to what the postmaster process will do during normal operation. If the POSIX implementation is unavailable or fails (excluding name collision retries), it falls back to System V shared memory on Unix-like systems or Windows-specific implementation on Windows. The function specifically avoids Solaris's  implementation due to known issues with sleeping and spurious failures under contention.

## Parameters / Member Variables
- None (void function returning a const char pointer to the implementation name)

## Dependencies
- Functions called/Symbols referenced:
  - pg_prng_seed (PostgreSQL pseudo-random number generator seed function)
  - pg_prng_uint32 (PostgreSQL pseudo-random number generator for 32-bit values)
  - getpid (system call to get process ID)
  - time (system call to get current time)
  - snprintf (standard C library function for formatted string creation)
  - shm_open (POSIX shared memory creation function)
  - close (system call to close file descriptor)
  - shm_unlink (POSIX shared memory removal function)
  - HAVE_SHM_OPEN (compile-time macro indicating POSIX shared memory support)
  - O_CREAT, O_RDWR, O_EXCL (file creation flags)
  - EEXIST (error code for file already exists)
- Called from (representative examples):
  - The initdb main initialization process (around line 1143)

## Notes and Other Information
- This is a static function, only accessible within initdb.c
- Returns one of three possible strings: "posix", "sysv", or "windows"
- Performs up to 10 retry attempts to handle potential name collisions when testing POSIX shared memory
- Uses a pseudo-random number generator seeded with PID and current time to generate unique shared memory segment names
- The function excludes Solaris from POSIX shared memory selection due to platform-specific reliability issues
- Critical for PostgreSQL's shared memory subsystem configuration during cluster initialization
- The chosen implementation affects how PostgreSQL allocates and manages shared memory throughout its runtime
- Test segment uses 0600 permissions (read/write for owner only) for security