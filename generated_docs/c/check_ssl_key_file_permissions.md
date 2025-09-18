# check_ssl_key_file_permissions

## Location
[src/backend/libpq/be-secure-common.c:114-177](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-common.c#L114-L177)

## Overview
Validates file permissions and ownership of SSL private key files to ensure security compliance before loading them.

## Definition
```c
bool check_ssl_key_file_permissions(const char *ssl_key_file, bool isServerStart)
```

## Detailed Description
This function performs comprehensive security checks on SSL private key files to prevent unauthorized access. It verifies that the key file exists, is a regular file, and has appropriate ownership and permissions. The function enforces strict security policies: files must be owned by either the database user or root, and must not have group or world access permissions. For user-owned files, permissions must be 0600 or more restrictive, while root-owned files may have 0640 or more restrictive permissions to allow group read access. These checks are only performed on Unix-like systems, as Windows permission models differ significantly.

## Parameters / Member Variables
- `ssl_key_file`: Path to the SSL private key file to be checked
- `isServerStart`: Boolean flag determining error message severity (FATAL for startup, LOG for runtime)

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call)
  - geteuid (system call)
  - S_ISREG (macro)
  - S_IRWXG, S_IRWXO, S_IWGRP, S_IXGRP (permission bit macros)
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md)

## Notes and Other Information
- Security checks are platform-specific and only enforced on Unix-like systems (not Windows/Cygwin)
- The function assumes PostgreSQL is not running as root user
- Similar permission checks exist in the client library (libpq) and should be kept in sync
- Root ownership allows slightly more permissive access (0640) to support system-wide certificate management
- Returns false on any security violation, true if all checks pass
- Error logging level is contextual: FATAL during server startup (preventing startup), LOG during runtime operations