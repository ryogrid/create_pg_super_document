# passwordFromFile

## Location
[src/interfaces/libpq/fe-connect.c:7425-7564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7425-L7564)

## Overview
Reads and parses a PostgreSQL password file (.pgpass) to retrieve a matching password for the specified connection parameters.

## Definition
```c
static char *passwordFromFile(const char *hostname, const char *port, const char *dbname, const char *username, const char *pgpassfile)
```

## Detailed Description
This function implements the PostgreSQL password file (.pgpass) lookup mechanism for libpq. It searches through the specified password file for an entry that matches the provided connection parameters (hostname, port, database name, and username) and returns the corresponding password.

The function performs several security checks before reading the password file:
- Verifies the file is a regular file (on Unix systems)
- Checks file permissions to ensure only the owner has access (mode 0600 or more restrictive on Unix)
- Handles platform-specific security considerations (Windows directory protection)

The password file format consists of lines with five colon-separated fields: hostname:port:database:username:password. Each field can contain wildcards (*) or escaped characters using backslashes. The function processes the file line by line, matching each field against the provided parameters using the pwdfMatchesString helper function.

When a matching entry is found, the password field is extracted, de-escaped (removing backslash escape sequences), and returned as a malloc'd string. The function also securely clears sensitive data from memory using explicit_bzero.

## Parameters / Member Variables
- `hostname`: The target hostname to match (NULL or empty string defaults to localhost)
- `port`: The target port number to match (NULL or empty string defaults to default PostgreSQL port)
- `dbname`: The target database name to match (required, function returns NULL if empty)
- `username`: The target username to match (required, function returns NULL if empty)
- `pgpassfile`: The path to the password file to read

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md)
  - DefaultHost
  - [is_unixsock_path](../i/is_unixsock_path.md)
  - DEFAULT_PGSOCKET_DIR
  - S_ISREG
  - [libpq_gettext](../l/libpq_gettext.md)
  - S_IRWXG, S_IRWXO
  - fopen
  - initPQExpBuffer
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md)
  - pg_strip_crlf
  - [pwdfMatchesString](pwdfMatchesString.md)
  - explicit_bzero
  - termPQExpBuffer
- Called from (representative examples):
  - internalPQconninfoOption (fe-connect.c:446)
  - pqConnectOptions2 (fe-connect.c:1332)

## Notes and Other Information
- This function is marked as static, indicating it's only used within the fe-connect.c file
- Returns a malloc'd string that must be freed by the caller
- Implements security checks to prevent reading from insecure password files
- Handles Unix socket paths by converting them to localhost for matching purposes
- Supports the standard PostgreSQL .pgpass file format with wildcard and escape sequence support
- Uses expandable buffers to handle arbitrarily long lines in the password file
- Performs secure memory cleanup using explicit_bzero to prevent password data from remaining in memory
- On Windows, relies on directory-level protection rather than file-level permission checks
- Comments and empty lines in the password file are ignored
- The function stops at the first matching entry found in the file