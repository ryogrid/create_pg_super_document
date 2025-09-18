# get_sock_dir

## Location
[src/bin/pg_upgrade/option.c:473-523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/option.c#L473-L523)

## Overview
Determines the appropriate socket directory for a PostgreSQL cluster, either from user configuration or by reading a live cluster's postmaster.pid file.

## Definition
```c
void get_sock_dir(ClusterInfo *cluster, bool live_check)
```

## Detailed Description
This function sets the socket directory for a PostgreSQL cluster with different behavior based on whether a live check is being performed. For non-live checks, it uses the user-specified socket directory. For live checks on Unix-like systems, it reads the postmaster.pid file to discover where the running server is listening, extracting both the port number and socket directory. This ensures pg_upgrade can connect to the live old cluster during upgrade operations. On Windows, socket directories are not used, so the function sets sockdir to NULL.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure to update with socket directory information
- `live_check`: Boolean indicating if this is for a live cluster connection (true) or new cluster setup (false)

## Dependencies
- Functions called/Symbols referenced:
  - fopen/fclose (for reading postmaster.pid)
  - snprintf (for path construction)
  - fgets (for reading file lines)
  - sscanf (for parsing port number)
  - [pg_strdup](../p/pg_strdup.md) (for string duplication)
  - pg_strip_crlf (to clean string endings)
  - [pg_fatal](../p/pg_fatal.md) (for error reporting)
  - [pg_log](../p/pg_log.md) (for warnings)
- Constants used:
  - LOCK_FILE_LINE_PORT (line number in postmaster.pid containing port)
  - LOCK_FILE_LINE_SOCKET_DIR (line number containing socket directory)
  - DEF_PGUPORT (default PostgreSQL port)
  - PG_WARNING (warning log level)
- Global variables:
  - user_opts.socketdir (user-specified socket directory)
  - old_cluster.port (may be updated from postmaster.pid)
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/pg_upgrade/pg_upgrade.c:132-133) - called for both old and new clusters

## Notes and Other Information
- Platform-specific: Unix-like systems use Unix domain sockets, Windows sets sockdir to NULL
- For live checks, reads postmaster.pid file format where specific line numbers contain port and socket directory
- Automatically corrects user-supplied port numbers if they differ from the running server
- Issues warnings when port number corrections occur
- Critical for pg_upgrade's ability to connect to existing running clusters during upgrade checks
- Socket directory resolution follows precedence: live server location > user specification > current directory