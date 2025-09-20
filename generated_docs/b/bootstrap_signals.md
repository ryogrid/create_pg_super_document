# bootstrap_signals

## Location
[src/backend/bootstrap/bootstrap.c:381-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L381-L407)

## Overview
bootstrap_signals configures signal handling for PostgreSQL bootstrap processes by setting all signals to their default behavior for simplified error handling.

## Definition
```c
static void bootstrap_signals(void)
```

## Detailed Description
bootstrap_signals sets up signal handling specifically for PostgreSQL's bootstrap mode operation. Unlike normal PostgreSQL backend processes that require sophisticated signal handling for inter-process communication and graceful shutdown, bootstrap processes use a simplified approach where all signals result in default system behavior (typically process termination).

The function explicitly sets four key signals (SIGHUP, SIGINT, SIGTERM, SIGQUIT) to their default handlers (SIG_DFL). This "curl up and die" approach is considered sufficient for bootstrap mode because bootstrap processes are typically short-lived initialization processes that don't need to handle complex shutdown scenarios or inter-process communication.

This simplified signal handling serves both functional and documentation purposes, making it clear that bootstrap processes intentionally use minimal signal handling compared to regular PostgreSQL backends.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md) (PostgreSQL's signal handling wrapper)
  - SIG_DFL (default signal handler constant)
  - SIGHUP (hangup signal constant)
  - SIGINT (interrupt signal constant) 
  - SIGTERM (termination signal constant)
  - SIGQUIT (quit signal constant)
- Called from (representative examples):
  - [BootstrapModeMain](../B/BootstrapModeMain.md)

## Notes and Other Information
- This is a static function, accessible only within bootstrap.c
- Includes an assertion to ensure it's not called under the postmaster (Assert(!IsUnderPostmaster))
- Uses pqsignal() instead of direct signal() calls for PostgreSQL compatibility
- The simplified signal handling reflects the fact that bootstrap processes don't need complex shutdown or communication mechanisms
- Located in src/backend/bootstrap/bootstrap.c:381-407
- Part of the "misc functions" section of the bootstrap module