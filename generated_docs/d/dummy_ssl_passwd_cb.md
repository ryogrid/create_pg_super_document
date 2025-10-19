# dummy_ssl_passwd_cb

## Location
[src/backend/libpq/be-secure-openssl.c:1136-1152](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1136-L1152)

## Overview
A dummy passphrase callback that returns an empty passphrase to prevent interactive prompting during automated SSL context operations.

## Definition

```c
static int
dummy_ssl_passwd_cb(char *buf, int size, int rwflag, void *userdata)
```
## Detailed Description
This function serves as a protective mechanism against OpenSSL's default behavior of prompting for passphrases on /dev/tty when encountering password-protected SSL certificates or keys. During automated operations like postmaster SIGHUP cycles or SSL context reloads in EXEC_BACKEND postmaster children, interactive prompting would cause system hangs or failures. This dummy callback intentionally returns an empty passphrase, which guarantees that the SSL key loading will fail gracefully rather than block waiting for user input. The function also sets a flag (dummy_ssl_passwd_cb_called) to enable more descriptive error reporting when this callback is invoked.

## Parameters / Member Variables
- `*buf`: Buffer to store the passphrase (receives empty string)
- `size`: Maximum size of the buffer (must be > 0)
- `rwflag`: Read/write flag (unused in this implementation)
- `*userdata`: User-defined data passed to the callback (unused)
## Dependencies
- Functions called/Symbols referenced:
  - None (only uses direct assignments and assertions)
- Called from (representative examples):
  - [default_openssl_tls_init](default_openssl_tls_init.md) (src/backend/libpq/be-secure-openssl.c:1766)

## Notes and Other Information
- Always returns 0 (empty passphrase length)
- Sets dummy_ssl_passwd_cb_called flag to true for error reporting purposes
- Prevents system blocking during automated SSL operations
- Ensures graceful failure rather than hanging on interactive prompts
- Critical for PostgreSQL's automated SSL management in server environments
- The empty passphrase guarantees failure, which is the intended behavior for security

## Simplified Source

```c
static int
dummy_ssl_passwd_cb(char *buf, int size, int rwflag, void *userdata)
{
    // Set flag to indicate this callback was used for error reporting
    dummy_ssl_passwd_cb_called = true;

    // Return empty string to guarantee failure
    Assert(size > 0);
    buf[0] = '\0';
    return 0;
}
```