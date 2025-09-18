# PQdefaultSSLKeyPassHook_OpenSSL

## Location
src/interfaces/libpq/fe-secure.c: 488 - 497

## Overview  
The default SSL private key password handler that retrieves passwords from the connection's sslpassword field for PostgreSQL client SSL connections.

## Definition
```c
int PQdefaultSSLKeyPassHook_OpenSSL(char *buf, int size, PGconn *conn)
```

## Detailed Description
This function provides the default implementation for handling SSL private key password prompts in PostgreSQL's libpq. It retrieves the password from the connection object's `sslpassword` field, which is typically set through connection parameters or connection strings.

The function serves as both the built-in fallback when no custom SSL key pass hook is registered, and as a reference implementation that applications can explicitly install to prevent OpenSSL from prompting for passwords on stdin. This is particularly useful in non-interactive environments or GUI applications where stdin prompting would be inappropriate.

When the stored password is longer than the available buffer, the function truncates it and warns the user via stderr. The function ensures the output buffer is always null-terminated for safe string handling.

## Parameters / Member Variables
- `buf`: Output buffer where the password will be written as a null-terminated string
- `size`: Maximum size of the buffer including space for the null terminator  
- `conn`: PostgreSQL connection object containing the sslpassword field

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_gettext](../l/libpq_gettext.md) (for localized warning messages)
  - strlen, strncpy, fprintf (standard C library functions)
- Called from (representative examples):
  - [PQssl_passwd_cb](PQssl_passwd_cb.md) (internal OpenSSL password callback at fe-secure-openssl.c:2113)
- Connection fields accessed:
  - conn->sslpassword

## Notes and Other Information  
- Returns the length of the password written to buf, or 0 if no password is available
- If conn is NULL or conn->sslpassword is NULL/empty, returns 0 and sets buf[0] to '\0'
- Truncates passwords that exceed buffer size and issues a warning to stderr
- This function matches the PQsslKeyPassHook_OpenSSL_type callback signature
- Used automatically when no custom SSL key pass hook is set via PQsetSSLKeyPassHook_OpenSSL()
- Applications can call this explicitly to bypass OpenSSL's default stdin prompting behavior
- Thread-safe as it only reads from the connection object and uses stack-based buffers
- The sslpassword field is typically populated from connection parameters like 'sslpassword' or connection string options