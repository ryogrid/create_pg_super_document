# pg_md5_encrypt

## Location
[src/common/md5_common.c:145-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5_common.c#L145-L173)

## Overview
Creates a PostgreSQL-style MD5 encrypted password by concatenating password and salt, then computing MD5 hash with "md5" prefix.

## Definition

```c
bool
pg_md5_encrypt(const char *passwd, const char *salt, size_t salt_len,
			   char *buf, const char **errstr)
```
## Detailed Description
This function implements PostgreSQL's MD5 password encryption scheme by concatenating a plaintext password with a salt value, computing the MD5 hash of the combined data, and formatting the result with a "md5" prefix followed by the 32-character hexadecimal hash. This is the standard format used for MD5-encrypted passwords in PostgreSQL's authentication system.

The function allocates a temporary buffer to hold the concatenated password and salt, with the salt positioned after the password to provide additional security against users who might know the salt value. After computing the hash using pg_md5_hash, it formats the output with the required "md5" prefix.

## Parameters / Member Variables
- Changing password for ryo.: Null-terminated string containing the plaintext password to encrypt
- : Salt data to append to the password (need not be null-terminated)
- : Length of the salt data in bytes
- : Output buffer to receive the formatted result "md5" + 32-hex-digits (must be at least 36 bytes)
- : Pointer to a const char pointer that will be set to an error message on failure, or NULL on success

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - [pg_md5_hash](pg_md5_hash.md)
  - strlen
  - memcpy
  - strcpy
  - free
- Called from (representative examples):
  - [encrypt_password](../e/encrypt_password.md)
  - [md5_crypt_verify](../m/md5_crypt_verify.md)
  - [plain_crypt_verify](plain_crypt_verify.md)
  - [pg_password_sendauth](pg_password_sendauth.md)
  - PQencryptPassword

## Notes and Other Information
- Returns true on success, false on failure
- Output format is always "md5" followed by 32 hexadecimal characters
- The output buffer must be at least 36 bytes (3 for "md5" + 32 for hash + 1 for null terminator)
- Uses malloc for temporary buffer allocation, requiring proper error handling for out-of-memory conditions
- Salt is intentionally placed after the password in the concatenated buffer for security reasons
- This is the standard PostgreSQL MD5 password encryption format used throughout the authentication system
- The function provides proper memory management, freeing the temporary buffer in all code paths
- Error conditions include memory allocation failures and any errors from the underlying pg_md5_hash function
- Widely used in PostgreSQL's authentication infrastructure for both server-side and client-side password processing