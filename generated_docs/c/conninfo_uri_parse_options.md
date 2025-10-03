# conninfo_uri_parse_options

## Location
[src/interfaces/libpq/fe-connect.c:6375-6615](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6375-L6615)

## Overview
Parses a PostgreSQL connection URI string according to RFC 3986 syntax and populates connection options with the extracted values.

## Definition

```c
static bool
conninfo_uri_parse_options(PQconninfoOption *options, const char *uri,
						   PQExpBuffer errorMessage)
```
## Detailed Description
This function is the actual URI parser that handles PostgreSQL connection URIs in the format:


The function supports several advanced features:
- IPv6 addresses enclosed in square brackets
- Multiple netloc:port specifications separated by commas
- Percent-encoding (%xy) in any URI parts
- Comprehensive error handling with detailed error messages

The parsing process involves:
1. Skipping the URI prefix (postgresql://)
2. Extracting user credentials if present (user:password@)
3. Parsing host specifications (including multiple hosts separated by commas)
4. Handling IPv6 addresses in brackets
5. Extracting port numbers
6. Parsing database name
7. Delegating query parameter parsing to conninfo_uri_parse_params

## Parameters / Member Variables
- `*options`: Array of PQconninfoOption structures to be populated with parsed values
- `*uri`: The connection URI string to parse
- `errorMessage`: Buffer to store error messages if parsing fails
## Dependencies
- Functions called/Symbols referenced:
  - [uri_prefix_length](../u/uri_prefix_length.md)
  - [conninfo_storeval](conninfo_storeval.md)
  - [conninfo_uri_parse_params](conninfo_uri_parse_params.md)
  - [libpq_append_error](../l/libpq_append_error.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)/termPQExpBuffer
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)/appendPQExpBufferChar
  - PQExpBufferDataBroken
- Called from (representative examples):
  - [conninfo_uri_parse](conninfo_uri_parse.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns true on successful parsing, false on error
- Uses dynamic memory allocation for URI manipulation (strdup)
- Supports PostgreSQL's extension allowing multiple host specifications
- Handles both IPv4 and IPv6 addresses with proper validation
- Avoids setting dbname to empty string to preserve default behavior
- All parsed values are URL-decoded before storage
- Memory cleanup is handled through a goto cleanup pattern

## Simplified Source

```c
static bool conninfo_uri_parse_options(PQconninfoOption *options, const char *uri,
                                       PQExpBuffer errorMessage) {
    char *uri_copy = strdup(uri);
    char *start, *p;
    char *user = NULL, *host = NULL;
    bool success = false;
    PQExpBufferData hostbuf, portbuf;

    if (!uri_copy) {
        libpq_append_error(errorMessage, "out of memory");
        return false;
    }

    // Initialize host and port buffers
    initPQExpBuffer(&hostbuf);
    initPQExpBuffer(&portbuf);

    // Skip URI prefix (postgresql://)
    int prefix_len = uri_prefix_length(uri);
    start = uri_copy + prefix_len;
    p = start;

    // Parse user credentials if present (user[:password]@)
    while (*p && *p != '@' && *p != '/') p++;
    if (*p == '@') {
        user = start;

        // Extract username
        while (*p != ':' && *p != '@') p++;
        char saved_char = *p;
        *p = '\0';

        if (*user && !conninfo_storeval(options, "user", user, errorMessage, false, true)) {
            goto cleanup;
        }

        // Extract password if present
        if (saved_char == ':') {
            char *password = p + 1;
            while (*p != '@') p++;
            *p = '\0';
            if (*password && !conninfo_storeval(options, "password", password, errorMessage, false, true)) {
                goto cleanup;
            }
        }
        p++;  // Skip past '@'
    } else {
        p = start;  // Reset if no credentials found
    }

    // Parse host specifications (may be multiple, comma-separated)
    while (true) {
        // Handle IPv6 addresses in brackets
        if (*p == '[') {
            host = ++p;
            while (*p && *p != ']') p++;
            if (!*p || p == host) {
                libpq_append_error(errorMessage, "Invalid IPv6 address in URI");
                goto cleanup;
            }
            *p++ = '\0';  // Terminate hostname and skip bracket
        } else {
            // Regular hostname or IPv4
            host = p;
            while (*p && *p != ':' && *p != '/' && *p != '?' && *p != ',') p++;
        }

        char terminator = *p;
        *p = '\0';
        appendPQExpBufferStr(&hostbuf, host);

        // Parse port if present
        if (terminator == ':') {
            char *port = ++p;
            while (*p && *p != '/' && *p != '?' && *p != ',') p++;
            terminator = *p;
            *p = '\0';
            appendPQExpBufferStr(&portbuf, port);
        }

        // Continue with next host or break
        if (terminator != ',') break;
        p++;
        appendPQExpBufferChar(&hostbuf, ',');
        appendPQExpBufferChar(&portbuf, ',');
    }

    // Store host and port values
    if (hostbuf.data[0] && !conninfo_storeval(options, "host", hostbuf.data, errorMessage, false, true)) {
        goto cleanup;
    }
    if (portbuf.data[0] && !conninfo_storeval(options, "port", portbuf.data, errorMessage, false, true)) {
        goto cleanup;
    }

    // Parse database name if present
    if (terminator && terminator != '?') {
        char *dbname = ++p;
        while (*p && *p != '?') p++;
        terminator = *p;
        *p = '\0';
        if (*dbname && !conninfo_storeval(options, "dbname", dbname, errorMessage, false, true)) {
            goto cleanup;
        }
    }

    // Parse query parameters if present
    if (terminator) {
        p++;
        if (!conninfo_uri_parse_params(p, options, errorMessage)) {
            goto cleanup;
        }
    }

    success = true;

cleanup:
    termPQExpBuffer(&hostbuf);
    termPQExpBuffer(&portbuf);
    free(uri_copy);
    return success;
}
```