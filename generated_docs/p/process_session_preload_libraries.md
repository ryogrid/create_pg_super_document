# process_session_preload_libraries

## Location
src/backend/utils/init/miscinit.c: 1912 - 1925

## Overview
process_session_preload_libraries loads both session_preload_libraries and local_preload_libraries during backend/session initialization, with different security restrictions for each.

## Definition
```c
void process_session_preload_libraries(void)
```

## Detailed Description
This function handles the loading of libraries that should be preloaded when a new PostgreSQL backend process starts (i.e., when a client session begins). It processes two different types of session-level preload libraries with different security models:

1. **session_preload_libraries**: These are loaded without path restrictions (restricted=false), similar to shared_preload_libraries but at the session level. This allows loading libraries from any location accessible to the PostgreSQL server.

2. **local_preload_libraries**: These are loaded with path restrictions (restricted=true), meaning they must be located in the $libdir/plugins/ directory. This provides a security mechanism to prevent users from loading arbitrary libraries from the filesystem.

The function is called during backend initialization, making these libraries available only to the specific session/backend process, unlike shared preload libraries which are available to all processes.

## Parameters / Member Variables
None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [load_libraries](../l/load_libraries.md) (called twice with different parameters)
  - session_preload_libraries_string (global variable)
  - local_preload_libraries_string (global variable)
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md)
  - INIT_PG_OVERRIDE_ROLE_LOGIN

## Notes and Other Information
- Called during backend initialization, typically in InitPostgres
- Loads libraries on a per-session basis, not server-wide like shared preload libraries
- Implements a two-tier security model: unrestricted session libraries and restricted local libraries
- The local_preload_libraries restriction to $libdir/plugins/ prevents potential security issues from user-controlled library loading
- [Session](../S/Session.md) preload libraries are useful for debugging tools, session-specific extensions, and per-connection customizations
- Libraries loaded here are not inherited by other sessions, providing isolation between different client connections
- The order of loading (session_preload_libraries first, then local_preload_libraries) may be significant for library dependencies