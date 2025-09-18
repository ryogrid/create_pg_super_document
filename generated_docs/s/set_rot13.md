# set_rot13

## Location
src/test/modules/ssl_passphrase_callback/ssl_passphrase_func.c: 56 - 66

## Overview
Hook function that configures OpenSSL to use a ROT13-based password callback for SSL certificate passphrase handling in PostgreSQL's SSL test module.

## Definition


## Detailed Description
This function serves as an OpenSSL TLS initialization hook that sets up a custom password callback for SSL certificate decryption. It's part of PostgreSQL's SSL passphrase callback test module that demonstrates how to programmatically provide SSL certificate passphrases instead of relying on external commands. The function warns users if they have configured the standard ssl_passphrase_command setting, as this module overrides that functionality. It then registers the rot13_passphrase function as the default password callback for the SSL context.

## Parameters / Member Variables
- : SSL_CTX pointer representing the OpenSSL context to configure
- : Boolean indicating whether this is called during server startup (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for warning messages)
  - errmsg (for error message formatting)
  - SSL_CTX_set_default_passwd_cb (OpenSSL function to set password callback)
  - rot13_passphrase (callback function for password processing)
  - ssl_passphrase_command (global configuration variable)
- Called from (representative examples):
  - _PG_init (via openssl_tls_init_hook assignment)
  - OpenSSL TLS initialization system

## Notes and Other Information
- Located in src/test/modules/ssl_passphrase_callback/ssl_passphrase_func.c:56-64
- This is a test/demonstration module, not intended for production use
- ROT13 transformation provides no real security - it's purely for testing purposes
- Warns users when ssl_passphrase_command is set, as this module overrides that functionality
- Part of PostgreSQL's SSL testing infrastructure for validating custom passphrase handling