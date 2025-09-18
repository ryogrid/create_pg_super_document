# cluster_conn_opts

## Location
src/bin/pg_upgrade/server.c: 92 - 121

## Overview
Generates standardized command-line connection options for external PostgreSQL utilities like psql and pg_dump.

## Definition
```c
char *cluster_conn_opts(ClusterInfo *cluster)
```

## Detailed Description
This function constructs command-line connection parameters that can be used with PostgreSQL client utilities. It builds a string containing host, port, and username options, with proper shell escaping for safety. The function uses a static buffer that persists between calls, making the result valid until the next invocation. Database name options are intentionally excluded due to inconsistent handling across different PostgreSQL utilities.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing server connection configuration

## Dependencies
- Functions called/Symbols referenced:
  - createPQExpBuffer
  - resetPQExpBuffer
  - appendPQExpBufferStr
  - appendShellString
  - appendPQExpBufferChar
  - appendPQExpBuffer
- Called from (representative examples):
  - generate_old_dump
  - prepare_new_cluster
  - prepare_new_globals
  - create_new_objects

## Notes and Other Information
- Uses static buffer management - result is valid only until next function call
- Includes proper shell escaping via appendShellString for security
- Intentionally omits database name to accommodate utility-specific handling differences
- Commonly used when spawning external PostgreSQL client processes during upgrade operations
- Conditional host parameter inclusion based on socket directory availability
- Part of pg_upgrade's external process execution infrastructure