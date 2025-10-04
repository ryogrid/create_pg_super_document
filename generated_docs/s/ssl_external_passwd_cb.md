# ssl_external_passwd_cb

## Location
[src/backend/libpq/be-secure-openssl.c:1116-1135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1116-L1135)

## Overview
A callback function that collects SSL certificate passphrases using an external command specified by ssl_passphrase_command.

## Definition

```c
static int
ssl_external_passwd_cb(char *buf, int size, int rwflag, void *userdata)
```
## Detailed Description
This function implements OpenSSL's password callback interface to retrieve passphrases for encrypted SSL certificates and private keys. It serves as a bridge between OpenSSL's internal passphrase requests and PostgreSQL's external passphrase command mechanism. The function uses the same prompt text as OpenSSL's internal password callback ("Enter PEM pass phrase:") to maintain consistency. It delegates the actual passphrase collection to run_ssl_passphrase_command(), which executes the command specified in the ssl_passphrase_command configuration parameter.

## Parameters / Member Variables
- `*buf`: Buffer to store the retrieved passphrase
- `size`: Maximum size of the buffer
- `rwflag`: Read/write flag (0 for reading, 1 for writing) - function asserts this is always 0
- `*userdata`: User-defined data passed to the callback (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [run_ssl_passphrase_command](../r/run_ssl_passphrase_command.md) (executes external command to get passphrase)
- Called from (representative examples):
  - [default_openssl_tls_init](../d/default_openssl_tls_init.md) (src/backend/libpq/be-secure-openssl.c:1752)
  - [default_openssl_tls_init](../d/default_openssl_tls_init.md) (src/backend/libpq/be-secure-openssl.c:1757)

## Notes and Other Information
- Returns the length of the passphrase retrieved, or -1 on error
- Uses Assert() to ensure rwflag is 0, as PostgreSQL only needs to read passphrases
- The prompt string matches OpenSSL's internal prompt for consistency
- Enables secure passphrase collection through external commands rather than interactive input
- Part of PostgreSQL's SSL certificate management system for automated deployments

## Simplified Source

```c
static int
ssl_external_passwd_cb(char *buf, int size, int rwflag, void *userdata)
{
    // Use OpenSSL's standard passphrase prompt
    const char *prompt = "Enter PEM pass phrase:";

    // Ensure we're only reading passphrases, not writing them
    Assert(rwflag == 0);

    // Execute external command to get passphrase
    return run_ssl_passphrase_command(prompt, ssl_is_server_start, buf, size);
}
```