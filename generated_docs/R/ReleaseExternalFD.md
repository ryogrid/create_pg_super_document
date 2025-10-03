# ReleaseExternalFD

## Location
[src/backend/storage/file/fd.c:1236-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1236-L1245)

## Overview
ReleaseExternalFD reports the release of an external file descriptor back to PostgreSQL's file descriptor management system, decrementing the count of externally held descriptors.

## Definition

```c
void
ReleaseExternalFD(void)
```
## Detailed Description
This function is the counterpart to ReserveExternalFD and is used to notify PostgreSQL's VFD management system when an externally managed file descriptor is no longer in use. It simply decrements the numExternalFDs counter that tracks how many file descriptors are being held externally.

The function includes an assertion to ensure that numExternalFDs is greater than zero before decrementing, helping to catch programming errors where releases don't match reservations. Importantly, this function is designed to never change errno, making it safe to use in error handling and cleanup paths.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - Assert (debug assertion)
- Called from (representative examples):
  - [XLogFileClose](../X/XLogFileClose.md) (WAL file operations)
  - [ClosePostmasterPorts](../C/ClosePostmasterPorts.md)
  - [dsm_impl_posix](../d/dsm_impl_posix.md) (shared memory operations)
  - [InitializeLatchSupport](../I/InitializeLatchSupport.md)
  - [CreateWaitEventSet](../C/CreateWaitEventSet.md)
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - [FreeWaitEventSetAfterFork](../F/FreeWaitEventSetAfterFork.md)
  - [libpqsrv_disconnect](../l/libpqsrv_disconnect.md)
  - [libpqsrv_connect_internal](../l/libpqsrv_connect_internal.md)

## Notes and Other Information
- Guaranteed not to change errno, making it safe for use in failure paths
- Must be called to match every successful ReserveExternalFD call
- Includes assertion to prevent underflow of numExternalFDs counter
- Part of the paired reservation/release mechanism for external FD tracking
- Used extensively in cleanup and error handling paths throughout PostgreSQL

## Simplified Source

```c
// Simplified version of ReleaseExternalFD
void ReleaseExternalFD(void) {
    // Verify we have external FDs to release
    Assert(numExternalFDs > 0);

    // Decrement the external FD counter
    numExternalFDs--;
}
```

Key simplifications made:
- Added explanatory comments for the two main actions
- The function is already very simple, so minimal changes were needed
- Preserved the essential assertion and counter decrement logic
- Maintained the errno-safe guarantee (no complex operations that could change errno)