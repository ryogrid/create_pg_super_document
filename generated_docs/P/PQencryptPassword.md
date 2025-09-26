# PQencryptPassword

## Location
src/interfaces/libpq/fe-auth.c: 1233 - 1275

## Overview
Legacy function that encrypts a password using MD5 hashing for PostgreSQL authentication, superseded by the more flexible PQencryptPasswordConn function.

## Definition


## Detailed Description
 is a deprecated convenience function that encrypts a plaintext password using MD5 hashing specifically for PostgreSQL authentication. It combines the password with the username as a salt to generate an MD5-hashed password string in the format expected by PostgreSQL's password authentication mechanism.

This function is equivalent to calling  with "md5" as the encryption method, but doesn't require a database connection object. The function has been deprecated in favor of , which supports multiple encryption methods and provides better security options.

The function allocates memory for the encrypted password string and returns it to the caller, who becomes responsible for freeing the memory. On failure (due to memory allocation or encryption errors), it returns NULL.

## Parameters / Member Variables
- Changing password for ryo.: Plaintext password to be encrypted
- : Username to be used as salt in the MD5 hashing process

## Dependencies
- Functions called/Symbols referenced:
  - malloc (memory allocation for result)
  - pg_md5_encrypt (core MD5 encryption functionality)
  - MD5_PASSWD_LEN (constant defining encrypted password length)
- Called from (representative examples):
  - Referenced in libpq-fe.h header definitions

## Notes and Other Information
- **DEPRECATED**: Use PQencryptPasswordConn instead for new applications
- Returns malloc'd memory that must be freed by caller
- Fixed to MD5 encryption only (no support for stronger algorithms)
- Allocates MD5_PASSWD_LEN + 1 bytes for the result string
- Returns NULL on memory allocation failure or encryption errors
- Not connection-dependent, can be used without active database connection
- MD5 is considered cryptographically weak; modern applications should use stronger methods
- Thread-safe assuming underlying pg_md5_encrypt is thread-safe