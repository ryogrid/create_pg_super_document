# BackgroundWorkerInitializeConnectionByOid

## Location
src/backend/postmaster/postmaster.c: 4191 - 4224

## Overview
Establishes a database connection for a background worker process using Object Identifiers (OIDs) instead of names for database and user identification.

## Definition


## Detailed Description
This function is the OID-based variant of BackgroundWorkerInitializeConnection. It initializes a database connection for a background worker process by calling InitPostgres() with database and user OIDs rather than names. This approach is more efficient when the OIDs are already known, as it avoids name lookup operations.

Like its name-based counterpart, the function performs validation checks to ensure the background worker is properly configured for database access and handles special connection flags that can bypass normal access restrictions. After establishing the connection, it transitions the process from initialization mode to normal processing mode.

## Parameters / Member Variables
- `dboid`: The Object Identifier (OID) of the database to connect to
- `useroid`: The Object Identifier (OID) of the user/role to connect as
- `flags`: Connection flags that can modify connection behavior:
  - BGWORKER_BYPASS_ALLOWCONN: Ignore datallowconn restrictions
  - BGWORKER_BYPASS_ROLELOGINCHECK: Ignore rolcanlogin restrictions

## Dependencies
- Functions called/Symbols referenced:
  - BackgroundWorker (struct type)
  - MyBgworkerEntry (global variable)
  - BGWORKER_BYPASS_ALLOWCONN (flag constant)
  - INIT_PG_OVERRIDE_ALLOW_CONNS (flag constant)
  - BGWORKER_BYPASS_ROLELOGINCHECK (flag constant)
  - INIT_PG_OVERRIDE_ROLE_LOGIN (flag constant)
  - BGWORKER_BACKEND_DATABASE_CONNECTION (flag constant)
  - InitPostgres (function)
  - IsInitProcessingMode (function)
  - SetProcessingMode (function)
  - NormalProcessing (processing mode constant)
- Called from (representative examples):
  - ParallelWorkerMain (src/backend/access/transam/parallel.c:1428)
  - InitializeLogRepWorker (src/backend/replication/logical/worker.c:4599)
  - worker_spi_main (src/test/modules/worker_spi/worker_spi.c:171)

## Notes and Other Information
- More efficient than the name-based version when OIDs are already available, avoiding name resolution overhead
- The function requires that the background worker was registered with BGWORKER_BACKEND_DATABASE_CONNECTION flag
- Passes NULL for both database name and username parameters to InitPostgres, relying entirely on OID-based identification
- The init_flags parameter never honors session_preload_libraries for security reasons
- Validates that the process remains in initialization mode until explicitly transitioned to normal processing
- Commonly used by parallel workers and logical replication workers where OIDs are readily available
- The function will terminate the process with FATAL error if connection requirements weren't indicated during registration