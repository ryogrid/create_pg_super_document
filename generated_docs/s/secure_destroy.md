# secure_destroy

## Location
[src/backend/libpq/be-secure.c:86-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure.c#L86-L96)

## Overview
Destroys the global security context and cleans up resources used by PostgreSQL's secure communication layer, serving as the cleanup counterpart to secure_initialize.

## Definition

```c
void
secure_destroy(void)
```
## Detailed Description
The `secure_destroy` function provides cleanup functionality for the security subsystem, specifically handling the destruction of SSL/TLS resources when PostgreSQL is compiled with SSL support. It acts as a simple wrapper around `be_tls_destroy` when SSL is available, ensuring proper cleanup of OpenSSL contexts, certificates, and other SSL-related resources. When SSL support is not compiled in, the function becomes a no-op.

This function is typically called during configuration reloads to clean up the old SSL configuration before initializing a new one, and during shutdown procedures to ensure proper resource cleanup.

## Parameters / Member Variables
- None: This function takes no parameters as it operates on global SSL context.

## Dependencies
- Functions called/Symbols referenced:
  - [be_tls_destroy](../b/be_tls_destroy.md) (when USE_SSL is defined)
  - USE_SSL (compile-time macro check)
- Called from (representative examples):
  - [process_pm_reload_request](../p/process_pm_reload_request.md) (during configuration reload to clean up old SSL context)
  - FeBeWaitSetNEvents (referenced in libpq.h)

## Notes and Other Information
- This function has no return value (void) as cleanup operations are expected to always succeed
- Should be called before calling secure_initialize during configuration reloads
- The function's behavior is entirely dependent on compile-time SSL support
- Part of PostgreSQL's resource management strategy for the security subsystem
- Safe to call multiple times or when no SSL context exists