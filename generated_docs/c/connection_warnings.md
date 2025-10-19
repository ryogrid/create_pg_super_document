# connection_warnings

## Location
[src/bin/psql/command.c:3912-3970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3912-L3970)

## Overview
Displays version information and compatibility warnings for the psql client-server connection, along with SSL and GSS security information.

## Definition

```c
void
connection_warnings(bool in_startup)
```
## Detailed Description
The  function provides user feedback about the established database connection, focusing on version compatibility and security status. It displays psql's version banner and warns users about potential compatibility issues when client and server versions don't match.

Key behaviors include:
- **Version banner display**: Shows psql program name and version, including server version if different from client
- **Compatibility warnings**: Alerts users when server major version is newer than client or when server version is unsupported (predates 9.2)
- **Startup vs. runtime behavior**: Displays full banner during startup, but only shows version mismatches for runtime connections
- **Security information**: Calls specialized functions to display SSL/TLS and GSS/SSPI security details
- **Platform-specific checks**: On Windows, performs codepage validation during startup
- **Quiet mode respect**: Suppresses output when psql is in quiet mode or non-terminal mode

## Parameters / Member Variables
- `in_startup`: Boolean indicating whether this is called during psql startup (true) or during a runtime connection change (false)
## Dependencies
- Functions called/Symbols referenced:
  - : Retrieves server version parameter from connection
  - : Formats PostgreSQL version numbers for display
  - : Validates Windows codepage settings (Windows only)
  - : Displays SSL/TLS connection information
  - : Displays GSS/SSPI authentication information
- Called from (representative examples):
  - : After establishing new connections
  - : When verifying connection status

## Notes and Other Information
- The function distinguishes between startup and runtime connection scenarios, providing different levels of information display
- Version compatibility checking uses major version comparison (e.g., 13.x vs 14.x) rather than exact version matching
- The support cutoff for old PostgreSQL servers is currently set at version 9.2
- Platform-specific behavior includes Windows codepage validation to ensure proper character encoding
- Security information display is delegated to specialized functions that handle the complexity of SSL/TLS and GSS/SSPI details
- Output is suppressed in quiet mode or when psql is not connected to a terminal, making it suitable for scripted use

## Simplified Source

```c
void connection_warnings(bool in_startup)
{
    // Only display warnings in interactive mode
    if (!pset.quiet && !pset.notty) {
        int client_ver = PG_VERSION_NUM;
        char client_verbuf[32];
        char server_verbuf[32];

        // Check version compatibility
        if (pset.sversion != client_ver) {
            // Get server version string
            const char *server_version = PQparameterStatus(pset.db, "server_version");
            if (!server_version) {
                formatPGVersionNumber(pset.sversion, true, server_verbuf, sizeof(server_verbuf));
                server_version = server_verbuf;
            }

            // Display version mismatch banner
            printf("%s (%s, server %s)\n", pset.progname, PG_VERSION, server_version);
        } else if (in_startup) {
            // Display normal banner only during startup when versions match
            printf("%s (%s)\n", pset.progname, PG_VERSION);
        }

        // Warn about major version compatibility issues
        if (pset.sversion / 100 > client_ver / 100 ||  // Server newer than client
            pset.sversion < 90200) {                    // Server too old (< 9.2)
            printf("WARNING: %s major version %s, server major version %s.\n"
                   "         Some psql features might not work.\n",
                   pset.progname,
                   formatPGVersionNumber(client_ver, false, client_verbuf, sizeof(client_verbuf)),
                   formatPGVersionNumber(pset.sversion, false, server_verbuf, sizeof(server_verbuf)));
        }

#ifdef WIN32
        // Check Windows codepage during startup
        if (in_startup)
            checkWin32Codepage();
#endif

        // Display security information
        printSSLInfo();
        printGSSInfo();
    }
}
```

**Simplified Logic:**
1. **Check display conditions**: Only show output in interactive, non-quiet mode
2. **Version comparison**: Compare client and server PostgreSQL versions
3. **Banner display**: Show program banner with version info, including server version if different
4. **Compatibility warnings**: Alert users about potential issues with version mismatches
5. **Platform checks**: Validate Windows codepage settings during startup
6. **Security info**: Display SSL/TLS and GSS/SSPI connection details

This function provides essential feedback about database connection status, version compatibility, and security configuration to help users understand their psql session context.