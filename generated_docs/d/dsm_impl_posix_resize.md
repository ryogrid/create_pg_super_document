# dsm_impl_posix_resize

## Location
[src/backend/storage/ipc/dsm_impl.c:351-422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/dsm_impl.c#L351-L422)

## Overview
Platform-specific helper function that sets the size of a POSIX shared memory segment file descriptor, with special handling for Linux tmpfs allocation to prevent SIGBUS errors.

## Definition

```c
static int
dsm_impl_posix_resize(int fd, off_t size)
```
## Detailed Description
The  function is responsible for setting the size of a shared memory segment associated with a file descriptor. It handles platform-specific concerns, particularly on Linux where POSIX shared memory is backed by tmpfs files.

On Linux systems with , it uses  to pre-allocate space rather than just extending the file size. This prevents the creation of sparse files (holes) that could cause SIGBUS errors when accessed later if tmpfs runs out of space.

The function includes signal handling to prevent interruption during the potentially long-running allocation process, only allowing SIGQUIT to interrupt while blocking other signals like SIGUSR1 that might cause excessive retries.

## Parameters / Member Variables
- : File descriptor of the shared memory segment to resize
- : Target size in bytes for the segment

## Dependencies
- Functions called/Symbols referenced:
  - posix_fallocate (on Linux with HAVE_POSIX_FALLOCATE)
  - ftruncate (fallback on other platforms)
  - sigprocmask (signal masking for uninterrupted operation)  
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end (wait event reporting)
- Called from:
  - [dsm_impl_posix](dsm_impl_posix.md) (during DSM_OP_CREATE operations)

## Notes and Other Information
- Returns 0 on success, non-zero on failure with errno set
- Uses conditional compilation for Linux-specific optimization
- Includes EINTR retry loops to handle signal interruption gracefully
- Blocks most signals during allocation to prevent excessive retries from conflicts
- On Linux, prevents SIGBUS by pre-allocating tmpfs pages rather than creating holes
- Wait events are reported for monitoring allocation time
- Critical for reliable shared memory operation on systems with limited tmpfs space

## Simplified Source

```c
// Simplified version of dsm_impl_posix_resize
static int dsm_impl_posix_resize(int fd, off_t size) {
    int rc;
    int save_errno;
    sigset_t save_sigmask;

    // Block signals to prevent interruption during allocation
    if (IsUnderPostmaster)
        sigprocmask(SIG_SETMASK, &BlockSig, &save_sigmask);

    pgstat_report_wait_start(WAIT_EVENT_DSM_ALLOCATE);

#if defined(HAVE_POSIX_FALLOCATE) && defined(__linux__)
    // On Linux: pre-allocate tmpfs pages to prevent SIGBUS
    do {
        rc = posix_fallocate(fd, 0, size);
    } while (rc == EINTR);

    errno = rc;  // posix_fallocate doesn't set errno
#else
    // Other platforms: simple file extension
    do {
        rc = ftruncate(fd, size);
    } while (rc < 0 && errno == EINTR);
#endif

    pgstat_report_wait_end();

    // Restore signal mask
    if (IsUnderPostmaster) {
        save_errno = errno;
        sigprocmask(SIG_SETMASK, &save_sigmask, NULL);
        errno = save_errno;
    }

    return rc;
}
```

Key simplifications made:
- Removed detailed comments while preserving essential logic flow
- Consolidated signal handling explanation into brief comments
- Maintained platform-specific conditional compilation
- Preserved critical error handling and retry loops
- Focused on the core allocation strategy differences between platforms