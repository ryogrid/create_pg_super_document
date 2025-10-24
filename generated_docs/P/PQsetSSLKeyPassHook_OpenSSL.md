# PQsetSSLKeyPassHook_OpenSSL

## Location
[src/interfaces/libpq/fe-secure.c:482-487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L482-L487)

## Overview
Sets a custom callback hook function for handling SSL private key password prompting in OpenSSL-enabled PostgreSQL client connections.

## Definition
```c
void PQsetSSLKeyPassHook_OpenSSL(PQsslKeyPassHook_OpenSSL_type hook)
```

## Detailed Description
This function allows applications to register a custom callback function for supplying passwords when PostgreSQL's libpq needs to decrypt SSL private key files during connection establishment. The hook function replaces the default password handling mechanism with application-specific logic.

When libpq needs to decrypt a client SSL certificate's private key, it will call the registered hook function instead of using the default password callback. This enables applications to implement custom password retrieval mechanisms, such as prompting users through a GUI, reading from secure storage, or implementing other security policies.

The hook function signature matches OpenSSL's pem_password_cb callback type, ensuring compatibility with OpenSSL's password callback mechanism.

## Parameters / Member Variables
- `hook`: A function pointer of type PQsslKeyPassHook_OpenSSL_type. This callback function will be called when a password is needed to decrypt an SSL private key. Pass NULL to unregister the current hook.

The hook function signature is:
```c
typedef int (*PQsslKeyPassHook_OpenSSL_type)(char *buf, int size, PGconn *conn);
```

Where:
- `buf`: Buffer to store the password (null-terminated string)
- `size`: Maximum size of the buffer including null terminator  
- `conn`: PostgreSQL connection handle for context

## Dependencies
- Functions called/Symbols referenced:
  - None (simple assignment operation)
- Called from (representative examples):
  - Client applications that need custom SSL key password handling
- Global variable modified:
  - PQsslKeyPassHook (static variable in fe-secure-openssl.c:96)

## Notes and Other Information
- The hook function should return the length of the password written to buf, or 0 if no password was provided
- The hook mechanism is only available when PostgreSQL is compiled with OpenSSL support
- Setting hook to NULL restores the default password behavior
- The hook function may be called multiple times during a single connection attempt if multiple encrypted keys need passwords
- This is part of PostgreSQL's libpq SSL/TLS functionality and requires proper SSL configuration
- Thread safety: Applications must ensure proper synchronization when setting hooks in multi-threaded environments

## Simplified Source

```c
void
PQsetSSLKeyPassHook_OpenSSL(PQsslKeyPassHook_OpenSSL_type hook)
{
    // Set the SSL key passphrase callback hook
    PQsslKeyPassHook = hook;
}
```