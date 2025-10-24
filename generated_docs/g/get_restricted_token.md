# get_restricted_token

## Location
[src/common/restricted_token.c:129-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/restricted_token.c#L129-L174)

## Overview
Ensures PostgreSQL utility programs run with a restricted Windows security token by re-executing themselves with reduced privileges when necessary.

## Definition

```c
void
get_restricted_token(void)
```
## Detailed Description
get_restricted_token is a security enforcement function that provides a cross-platform interface for privilege reduction, with Windows-specific implementation. On Windows, it implements a "run-once" pattern that:

1. **Environment Check**: Inspects the PG_RESTRICT_EXEC environment variable to determine if already running with restrictions
2. **Self Re-execution**: If not restricted, re-launches the current program using CreateRestrictedProcess() with a restricted security token
3. **Process Coordination**: Waits for the restricted child process to complete and propagates its exit code
4. **Privilege Inheritance**: The re-executed process inherits significantly reduced privileges and capabilities

This design allows PostgreSQL utilities (initdb, pg_resetwal, pg_rewind, etc.) to automatically drop dangerous privileges on Windows without requiring explicit administrative configuration. On non-Windows platforms, this function is a no-op.

The function uses an environment variable flag to prevent infinite recursion during the re-execution process.

## Parameters / Member Variables

## Dependencies  
- Functions called/Symbols referenced:
  - [CreateRestrictedProcess](../C/CreateRestrictedProcess.md)
  - setenv
  - [pg_free](../p/pg_free.md)
- Called from (representative examples):
  - [main](../m/main.md) (in initdb, pg_resetwal, pg_rewind, pg_upgrade, pg_createsubscriber)
  - [regression_main](../r/regression_main.md)

## Notes and Other Information
- **Cross-Platform**: No-op on non-Windows systems, Windows-specific security implementation
- **Self-Modification**: Changes its own execution environment through process re-launch
- **Recursion Prevention**: Uses PG_RESTRICT_EXEC environment variable to avoid infinite re-execution loops  
- **Exit Code Propagation**: Transparently passes through the exit status of the restricted child process
- **One-Time Operation**: Only performs restriction on the first execution, subsequent calls are no-ops
- **Utility Integration**: Commonly called early in main() functions of PostgreSQL command-line utilities
- **Security Model**: Part of PostgreSQL's defense-in-depth strategy for Windows privilege management
- **Process Lifecycle**: Parent process terminates after child completion, making the restriction transparent to callers

## Simplified Source

```c
void get_restricted_token(void) {
#ifdef WIN32
    // Check if we're already running with restrictions
    char *restrict_env = getenv("PG_RESTRICT_EXEC");
    if (restrict_env == NULL || strcmp(restrict_env, "1") != 0) {
        // Not restricted yet - re-execute with restricted token
        PROCESS_INFORMATION pi;
        char *cmdline = pg_strdup(GetCommandLine());

        // Mark that we're about to restrict
        setenv("PG_RESTRICT_EXEC", "1", 1);

        // Create restricted process
        HANDLE restrictedToken = CreateRestrictedProcess(cmdline, &pi);
        if (restrictedToken == 0) {
            pg_log_error("could not re-execute with restricted token: error code %lu",
                        GetLastError());
        } else {
            // Wait for restricted child and exit with its code
            CloseHandle(restrictedToken);
            CloseHandle(pi.hThread);
            WaitForSingleObject(pi.hProcess, INFINITE);

            DWORD exit_code;
            if (!GetExitCodeProcess(pi.hProcess, &exit_code)) {
                pg_fatal("could not get exit code from subprocess: error code %lu",
                        GetLastError());
            }
            exit(exit_code);
        }
        pg_free(cmdline);
    }
    // If we reach here, we're already running restricted
#endif
    // Non-Windows platforms: do nothing
}
```