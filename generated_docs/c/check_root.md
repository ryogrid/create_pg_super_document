# check_root

## Location
[src/backend/main/main.c:381-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/main/main.c#L381-L437)

## Overview
Security validation function that prevents PostgreSQL server from running with administrator/root privileges to avoid potential system security compromises.

## Definition

```c
static void
check_root(const char *progname)
```
## Detailed Description
This function implements a critical security check that prevents PostgreSQL from running with elevated privileges. It performs platform-specific checks to ensure the server process is not executing with administrator or root privileges, which could pose significant security risks.

The function performs different checks based on the platform:

**Unix/Linux Systems:**
1. **Root Check**: Verifies that the effective user ID is not root (UID 0)
2. **Setuid Check**: Ensures that real and effective user IDs match to prevent setuid-based privilege escalation attacks

**Windows Systems:**
- **Administrator Check**: Uses pgwin32_is_admin() to verify the process is not running with administrator privileges

If any of these security conditions are violated, the function immediately terminates the process with an error message directing users to the documentation for proper server startup procedures.

## Parameters / Member Variables
- `*progname`: The program name used in error messages to identify which PostgreSQL component failed the check
## Dependencies
- Functions called/Symbols referenced:
  - geteuid (get effective user ID, Unix/Linux)
  - getuid (get real user ID, Unix/Linux) 
  - [pgwin32_is_admin](../p/pgwin32_is_admin.md) (check for administrator privileges, Windows)
  - [write_stderr](../w/write_stderr.md) (output error messages)
  - exit (terminate process with error status)
- Called from:
  - [main](../m/main.md) (during startup security validation, unless bypassed for read-only operations)

## Notes and Other Information
- This function is static and only accessible within the main.c source file
- The check can be bypassed for certain read-only operations like --describe-config and -C parameter queries
- On Unix/Linux systems, the setuid check prevents a common security vulnerability where a setuid program could potentially escalate back to root privileges
- The function provides clear error messages explaining why running as root/administrator is prohibited
- Process termination (exit(1)) is immediate and non-recoverable when security violations are detected
- Platform-specific implementation uses conditional compilation (#ifndef WIN32, #else) to handle differences between Unix/Linux and Windows privilege models
- This security measure is fundamental to PostgreSQL's security architecture and cannot be disabled through configuration options