# pg_strong_random_init

## Location
[src/port/pg_strong_random.c:147-152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/pg_strong_random.c#L147-L152)

## Overview
Initializes the cryptographically secure random number generator before any calls to pg_strong_random are made.

## Definition

```c
void
pg_strong_random_init(void)
```
## Detailed Description
The pg_strong_random_init function initializes the cryptographically secure random number generator that will be used by pg_strong_random. The implementation varies depending on the available system facilities:

1. **OpenSSL builds**: Calls RAND_poll() to ensure processes do not share OpenSSL randomness state. This is required for OpenSSL versions prior to 1.1.1 to maintain security across process boundaries.

2. **Windows builds**: No initialization is needed as the Windows CryptGenRandom API handles initialization internally.

3. **Other platforms**: No initialization is needed as the implementation directly reads from /dev/urandom.

This function must be called once per process before any calls to pg_strong_random. It's designed to run early in postmaster and backend startup, so it cannot rely on backend infrastructure like elog() or palloc().

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - RAND_poll (OpenSSL builds only)
- Called from (representative examples):
  - [fork_process](../f/fork_process.md)

## Notes and Other Information
- Must be called before any use of pg_strong_random in each process
- Implementation is conditional based on compile-time flags (USE_OPENSSL, WIN32)
- Designed for early startup usage without backend infrastructure dependencies
- For OpenSSL builds, ensures proper random state isolation between processes
- Safe to call multiple times (no-op in most implementations)