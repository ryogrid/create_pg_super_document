# default_openssl_tls_init

## Location
[src/backend/libpq/be-secure-openssl.c:1747-1768](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1747-L1768)

## Overview
Configures OpenSSL password callback handlers for SSL/TLS contexts based on server startup state and passphrase command configuration.

## Definition

```c
static void
default_openssl_tls_init(SSL_CTX *context, bool isServerStart)
```
## Detailed Description
The `default_openssl_tls_init` function is responsible for setting up appropriate password callback functions in the OpenSSL SSL context based on whether the server is starting up or reloading configuration. It handles two primary scenarios:

1. **Server startup**: When the server is initially starting (`isServerStart = true`), it sets up the external password callback if a passphrase command is configured.

2. **Configuration reload**: When the server is reloading configuration (`isServerStart = false`), it intelligently chooses between:
   - Using the external password callback if both a passphrase command is configured AND the command supports reload operations
   - Using a dummy password callback that prevents interactive prompts in an already-running server when no external command is available or reload is not supported

This design ensures that PostgreSQL servers can handle SSL certificate passphrases appropriately without hanging on interactive prompts during runtime operations.

## Parameters / Member Variables
- `context`: The OpenSSL SSL_CTX structure to configure with the appropriate password callback
- `isServerStart`: Boolean flag indicating whether this is called during server startup (true) or configuration reload (false)

## Dependencies
- Functions called/Symbols referenced:
  - [ssl_external_passwd_cb](../s/ssl_external_passwd_cb.md): External password callback function that executes configured passphrase commands
  - [dummy_ssl_passwd_cb](dummy_ssl_passwd_cb.md): Dummy callback that prevents interactive password prompts
  - `SSL_CTX_set_default_passwd_cb`: OpenSSL function to set the default password callback
- Global variables referenced:
  - `ssl_passphrase_command`: Array containing the configured passphrase command
  - `ssl_passphrase_command_supports_reload`: Boolean indicating if the passphrase command supports reload operations
- Called from:
  - No direct references found in the codebase (likely used through function pointers or indirect calls)

## Notes and Other Information
- This is a static function local to the be-secure-openssl.c file, indicating it's an internal implementation detail of PostgreSQL's OpenSSL integration
- The function is designed to prevent server hangs during configuration reloads by avoiding interactive password prompts in running servers
- The logic carefully distinguishes between server startup (where interactive prompts might be acceptable) and runtime reloads (where they must be avoided)
- Located in src/backend/libpq/be-secure-openssl.c at lines 1747-1768
- Part of PostgreSQL's SSL/TLS security infrastructure for handling encrypted private keys

## Simplified Source

```c
static void default_openssl_tls_init(SSL_CTX *context, bool isServerStart)
{
    if (isServerStart)
    {
        // Server startup: Use external password callback if configured
        if (ssl_passphrase_command[0])
            SSL_CTX_set_default_passwd_cb(context, ssl_external_passwd_cb);
    }
    else
    {
        // Configuration reload: Choose appropriate callback
        if (ssl_passphrase_command[0] && ssl_passphrase_command_supports_reload)
            SSL_CTX_set_default_passwd_cb(context, ssl_external_passwd_cb);
        else
            // Prevent interactive prompts in running server
            SSL_CTX_set_default_passwd_cb(context, dummy_ssl_passwd_cb);
    }
}
```