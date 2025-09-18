# dummy_ssl_passwd_cb

## Location
src/backend/libpq/be-secure-openssl.c: 1136 - 1152

## Overview
A dummy passphrase callback that returns an empty passphrase to prevent interactive prompting during automated SSL context operations.

## Definition


## Detailed Description
This function serves as a protective mechanism against OpenSSL's default behavior of prompting for passphrases on /dev/tty when encountering password-protected SSL certificates or keys. During automated operations like postmaster SIGHUP cycles or SSL context reloads in EXEC_BACKEND postmaster children, interactive prompting would cause system hangs or failures. This dummy callback intentionally returns an empty passphrase, which guarantees that the SSL key loading will fail gracefully rather than block waiting for user input. The function also sets a flag (dummy_ssl_passwd_cb_called) to enable more descriptive error reporting when this callback is invoked.

## Parameters / Member Variables
- : Buffer to store the passphrase (receives empty string)
- : Maximum size of the buffer (must be > 0)
- : Read/write flag (unused in this implementation)
- : User-defined data passed to the callback (unused)

## Dependencies
- Functions called/Symbols referenced:
  - None (only uses direct assignments and assertions)
- Called from (representative examples):
  - default_openssl_tls_init (src/backend/libpq/be-secure-openssl.c:1766)

## Notes and Other Information
- Always returns 0 (empty passphrase length)
- Sets dummy_ssl_passwd_cb_called flag to true for error reporting purposes
- Prevents system blocking during automated SSL operations
- Ensures graceful failure rather than hanging on interactive prompts
- Critical for PostgreSQL's automated SSL management in server environments
- The empty passphrase guarantees failure, which is the intended behavior for security