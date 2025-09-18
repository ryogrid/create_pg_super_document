# connection_warnings

## Location
[src/bin/psql/command.c:3912-3970](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3912-L3970)

## Overview
Displays version information and compatibility warnings for the psql client-server connection, along with SSL and GSS security information.

## Definition


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
- : Boolean indicating whether this is called during psql startup (true) or during a runtime connection change (false)

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