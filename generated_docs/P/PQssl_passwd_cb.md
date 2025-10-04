# PQssl_passwd_cb

## Location
[src/interfaces/libpq/fe-secure-openssl.c:2106-2126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L2106-L2126)

## Overview
Internal OpenSSL callback function that supplies passwords to decrypt client SSL certificates and private keys.

## Definition

```c
static int
PQssl_passwd_cb(char *buf, int size, int rwflag, void *userdata)
```
## Detailed Description
This function serves as the interface between OpenSSL's password callback mechanism and libpq's SSL key passphrase handling system. It matches the OpenSSL `pem_password_cb` function signature and is registered with OpenSSL to be called when a passphrase is needed to decrypt an encrypted private key file. The function implements a two-tier approach: if a custom hook has been registered via `PQsetSSLKeyPassHook_OpenSSL`, it delegates to that hook; otherwise, it falls back to the default behavior implemented by `PQdefaultSSLKeyPassHook_OpenSSL`.

## Parameters / Member Variables
- `buf`: Character buffer to write the password/passphrase into
- `size`: Maximum size of the buffer (including null terminator)
- `rwflag`: OpenSSL read/write flag (not used by this implementation)
- `userdata`: User data pointer, expected to be a PGconn* connection structure

## Dependencies
- Functions called/Symbols referenced:
  - PQsslKeyPassHook (static variable check)
  - [PQdefaultSSLKeyPassHook_OpenSSL](PQdefaultSSLKeyPassHook_OpenSSL.md)
- Called from (representative examples):
  - [initialize_SSL](../i/initialize_SSL.md) (during SSL context setup)
  - OpenSSL internal key loading functions

## Notes and Other Information
- This is a static function internal to fe-secure-openssl.c
- Must conform to OpenSSL's `pem_password_cb` typedef for compatibility
- The rwflag parameter is ignored in this implementation 
- Returns the length of the password written to buf, or 0 if no password could be provided
- Located in fe-secure-openssl.c:2106-2120
- Provides the bridge between OpenSSL's callback system and libpq's configurable hook mechanism

## Simplified Source

```c
static int PQssl_passwd_cb(char *buf, int size, int rwflag, void *userdata) {
    PGconn *conn = userdata;

    // Use custom hook if available, otherwise use default
    if (PQsslKeyPassHook) {
        return PQsslKeyPassHook(buf, size, conn);
    } else {
        return PQdefaultSSLKeyPassHook_OpenSSL(buf, size, conn);
    }
}
```