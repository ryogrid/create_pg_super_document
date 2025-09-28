# pg_GSS_load_servicename

## Location
[src/interfaces/libpq/fe-gssapi-common.c:82-128](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-gssapi-common.c#L82-L128)

## Overview
Client-side function that constructs and imports a GSS-API service principal name for the target PostgreSQL server.

## Definition
```c
int pg_GSS_load_servicename(PGconn *conn)
```

## Detailed Description
This function creates a GSS-API service principal name by combining the Kerberos service name with the target host name. The resulting name follows the format "service@hostname" and is imported into GSS-API as a host-based service name. This is essential for GSS-API authentication as it identifies the specific service instance the client wants to authenticate to.

The function first checks if a target name has already been loaded to avoid redundant work. It validates that a hostname is available, then constructs the service principal name string and imports it using `gss_import_name`. The imported name is stored in the connection object for use in subsequent authentication operations.

## Parameters / Member Variables
- `conn`: PostgreSQL connection object containing connection parameters and state

## Dependencies
- Functions called/Symbols referenced:
  - [PQhost](../P/PQhost.md) (gets hostname from connection)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (error reporting)
  - malloc (memory allocation)
  - snprintf (string formatting)
  - strlen (string length)
  - gss_import_name (GSS-API function)
  - free (memory deallocation)
  - [pg_GSS_error](pg_GSS_error.md) (GSS error reporting)
  - [libpq_gettext](../l/libpq_gettext.md) (internationalization)
  - gss_buffer_desc (GSS-API type)
  - GSS_C_NT_HOSTBASED_SERVICE (GSS-API name type)
  - STATUS_OK, STATUS_ERROR (PostgreSQL status constants)
- Called from (representative examples):
  - [pg_GSS_startup](pg_GSS_startup.md) (authentication startup)
  - [pqsecure_open_gss](pqsecure_open_gss.md) (secure connection establishment)

## Notes and Other Information
- This is a client-side only function (libpq)
- The function is idempotent - it checks if the target name is already loaded
- Memory allocation is handled carefully with proper cleanup on error
- The service principal name format is "krbsrvname@hostname"
- Requires a valid hostname to be specified in the connection
- The imported name is stored in conn->gtarg_nam for later use
- Error messages are properly internationalized using libpq_gettext
- Memory for the temporary buffer is allocated and freed within the function

## Simplified Source

```c
// Simplified version of pg_GSS_load_servicename
int pg_GSS_load_servicename(PGconn *conn) {
    OM_uint32 maj_stat, min_stat;
    int maxlen;
    gss_buffer_desc temp_gbuf;
    char *host;

    // Check if service name already loaded
    if (conn->gtarg_nam != NULL)
        return STATUS_OK;

    // Get hostname from connection
    host = PQhost(conn);
    if (!(host && host[0] != '\0')) {
        libpq_append_conn_error(conn, "host name must be specified");
        return STATUS_ERROR;
    }

    // Construct service principal name: "service@hostname"
    maxlen = strlen(conn->krbsrvname) + strlen(host) + 2;
    temp_gbuf.value = malloc(maxlen);
    if (!temp_gbuf.value) {
        libpq_append_conn_error(conn, "out of memory");
        return STATUS_ERROR;
    }

    snprintf(temp_gbuf.value, maxlen, "%s@%s", conn->krbsrvname, host);
    temp_gbuf.length = strlen(temp_gbuf.value);

    // Import the service name into GSS-API
    maj_stat = gss_import_name(&min_stat, &temp_gbuf,
                              GSS_C_NT_HOSTBASED_SERVICE, &conn->gtarg_nam);
    free(temp_gbuf.value);

    // Check for GSS-API errors
    if (maj_stat != GSS_S_COMPLETE) {
        pg_GSS_error(libpq_gettext("GSSAPI name import error"),
                     conn, maj_stat, min_stat);
        return STATUS_ERROR;
    }

    return STATUS_OK;
}
```

Key simplifications made:
- Added clear comments for each major step of the process
- Emphasized the service principal name format construction
- Made the error handling flow more explicit
- Highlighted the memory management pattern
- Focused on the core functionality of service name preparation for GSS authentication