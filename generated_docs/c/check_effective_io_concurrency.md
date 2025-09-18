# check_effective_io_concurrency

## Location
[src/backend/commands/variable.c:1223-1235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1223-L1235)

## Overview
A GUC (Grand Unified Configuration) check hook function that validates the effective_io_concurrency parameter, ensuring it is set to 0 on platforms that lack posix_fadvise() support.

## Definition


## Detailed Description
This function serves as a check hook for the effective_io_concurrency GUC parameter in PostgreSQL. The effective_io_concurrency parameter controls the number of concurrent disk I/O operations that PostgreSQL should expect for a single table scan. However, this functionality requires the posix_fadvise() system call for prefetching data.

On platforms where USE_PREFETCH is not defined (indicating lack of posix_fadvise() support), the function enforces that effective_io_concurrency must be set to 0, as the prefetching mechanism cannot function without posix_fadvise(). When prefetching is available, any non-negative value is accepted.

## Parameters / Member Variables
- : Pointer to the new integer value being set for the effective_io_concurrency parameter
- : Pointer to extra data (unused in this function, can be NULL)
- : The source of the configuration change (GucSource enum value)

## Dependencies
- Functions called/Symbols referenced:
  - GUC_check_errdetail (macro for setting detailed GUC error messages)
  - GucSource (enum type for configuration sources)
  - USE_PREFETCH (compile-time macro indicating posix_fadvise() availability)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H header file for GUC system integration

## Notes and Other Information
- This is a platform-dependent validation that only restricts values when USE_PREFETCH is not available
- The effective_io_concurrency parameter is used by PostgreSQL's bitmap heap scans to optimize I/O patterns
- posix_fadvise() is a POSIX system call that provides hints to the kernel about expected file access patterns
- Returns true to accept the new value, false to reject it
- Uses GUC_check_errdetail to provide specific error information about platform limitations
- On platforms with prefetch support, the actual validation of reasonable ranges is handled elsewhere in the GUC system