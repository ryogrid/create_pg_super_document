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
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md)
  - [pg_strip_crlf](pg_strip_crlf.md)
  - [pwdfMatchesString](pwdfMatchesString.md)
  - [explicit_bzero](../e/explicit_bzero.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Called from (representative examples):
  - internalPQconninfoOption (fe-connect.c:446)
  - [pqConnectOptions2](pqConnectOptions2.md) (fe-connect.c:1332)

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

## Simplified Source

```c
static char *
passwordFromFile(const char *hostname, const char *port, const char *dbname,
                 const char *username, const char *pgpassfile)
{
    FILE *fp;
    struct stat stat_buf;
    PQExpBufferData buf;

    // Validate required parameters
    if (!dbname || !dbname[0] || !username || !username[0])
        return NULL;

    // Normalize hostname and port
    if (!hostname || !hostname[0])
        hostname = DefaultHost;
    else if (is_unixsock_path(hostname) && strcmp(hostname, DEFAULT_PGSOCKET_DIR) == 0)
        hostname = DefaultHost;

    if (!port || !port[0])
        port = DEF_PGPORT_STR;

    // Security checks
    if (stat(pgpassfile, &stat_buf) != 0)
        return NULL;

#ifndef WIN32
    if (!S_ISREG(stat_buf.st_mode) || (stat_buf.st_mode & (S_IRWXG | S_IRWXO))) {
        // File security warnings omitted
        return NULL;
    }
#endif

    fp = fopen(pgpassfile, "r");
    if (!fp)
        return NULL;

    initPQExpBuffer(&buf);

    // Read file line by line
    while (!feof(fp) && !ferror(fp)) {
        // Read line into buffer
        if (!enlargePQExpBuffer(&buf, 128))
            break;

        if (fgets(buf.data + buf.len, buf.maxlen - buf.len, fp) == NULL)
            break;

        buf.len += strlen(buf.data + buf.len);

        // Process complete lines
        if (!(buf.len > 0 && buf.data[buf.len - 1] == '\n') && !feof(fp))
            continue;

        // Skip comments
        if (buf.data[0] != '#') {
            char *t = buf.data;
            int len = pg_strip_crlf(t);

            // Match all fields: hostname:port:dbname:username:password
            if (len > 0 &&
                (t = pwdfMatchesString(t, hostname)) != NULL &&
                (t = pwdfMatchesString(t, port)) != NULL &&
                (t = pwdfMatchesString(t, dbname)) != NULL &&
                (t = pwdfMatchesString(t, username)) != NULL) {

                // Found match - extract and de-escape password
                char *ret = strdup(t);
                fclose(fp);
                explicit_bzero(buf.data, buf.maxlen);
                termPQExpBuffer(&buf);

                if (ret) {
                    // De-escape password (remove backslash escapes)
                    char *p1, *p2;
                    for (p1 = p2 = ret; *p1 != ':' && *p1 != '\0'; ++p1, ++p2) {
                        if (*p1 == '\\' && p1[1] != '\0')
                            ++p1;
                        *p2 = *p1;
                    }
                    *p2 = '\0';
                }

                return ret;
            }
        }

        buf.len = 0;  // Reset for next line
    }

    // Cleanup
    fclose(fp);
    explicit_bzero(buf.data, buf.maxlen);
    termPQExpBuffer(&buf);
    return NULL;
}
```