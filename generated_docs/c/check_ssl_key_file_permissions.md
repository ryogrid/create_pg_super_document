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

## Simplified Source

```c
// Simplified version of check_ssl_key_file_permissions
bool check_ssl_key_file_permissions(const char *ssl_key_file, bool isServerStart) {
    int loglevel = isServerStart ? FATAL : LOG;
    struct stat file_stats;

    // Check if key file exists and is accessible
    if (stat(ssl_key_file, &file_stats) != 0) {
        ereport(loglevel, "could not access private key file");
        return false;
    }

    // Key file must be a regular file (not directory, symlink, etc.)
    if (!S_ISREG(file_stats.st_mode)) {
        ereport(loglevel, "private key file is not a regular file");
        return false;
    }

#if !defined(WIN32) && !defined(__CYGWIN__)
    // Security checks for Unix-like systems only

    // File must be owned by database user or root
    if (file_stats.st_uid != geteuid() && file_stats.st_uid != 0) {
        ereport(loglevel, "private key file must be owned by database user or root");
        return false;
    }

    // Check permissions based on ownership
    bool has_unsafe_permissions;
    if (file_stats.st_uid == geteuid()) {
        // User-owned: no group or world access allowed (max 0600)
        has_unsafe_permissions = (file_stats.st_mode & (S_IRWXG | S_IRWXO));
    } else {
        // Root-owned: no world access, limited group access (max 0640)
        has_unsafe_permissions = (file_stats.st_mode & (S_IWGRP | S_IXGRP | S_IRWXO));
    }

    if (has_unsafe_permissions) {
        ereport(loglevel, "private key file has group or world access");
        return false;
    }
#endif

    return true;
}
```

Key simplifications made:
- Consolidated error reporting into simpler messages
- Renamed variables for clarity (`buf` → `file_stats`)
- Added explanatory comments for each validation step
- Simplified permission checking logic with descriptive variable
- Removed detailed error messages and platform-specific comments
- Focused on the main security validation workflow