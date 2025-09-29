# explicit_bzero

## Location
[src/port/explicit_bzero.c:52-57](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/explicit_bzero.c#L52-L57)

## Overview
A secure memory clearing function that ensures sensitive data is properly zeroed from memory buffers, preventing it from being recovered or optimized away by compilers.

## Definition
void explicit_bzero(void *buf, size_t len)

## Detailed Description
explicit_bzero provides a secure way to clear sensitive data from memory buffers. Unlike regular memset(), this function is designed to resist compiler optimizations that might eliminate "dead stores" - memory writes that the compiler believes are unnecessary because the memory won't be read again.

The function has three different implementations depending on the target platform:

1. **Systems with memset_s()**: Uses the C11 Annex K function memset_s() which is guaranteed not to be optimized away
2. **Windows systems**: Uses SecureZeroMemory() which provides similar guarantees on Windows platforms  
3. **Other systems**: Uses an indirect function call through a volatile pointer (bzero_p) to call bzero2(), making it difficult for compilers to optimize away the memory clearing operation

This multi-platform approach ensures that sensitive data such as passwords, cryptographic keys, and other security-critical information is reliably cleared from memory across different operating systems and compiler implementations.

## Parameters / Member Variables
- buf: Pointer to the memory buffer to be securely cleared
- len: Size in bytes of the buffer to be cleared

## Dependencies
- Functions called/Symbols referenced:
  - memset_s (on systems with HAVE_DECL_MEMSET_S)
  - SecureZeroMemory (on WIN32 systems)
  - bzero_p (volatile function pointer to bzero2 on other systems)
- Called from (representative examples):
  - [run_ssl_passphrase_command](../r/run_ssl_passphrase_command.md) (src/backend/libpq/be-secure-common.c:68,80,90)
  - [pg_cryptohash_free](../p/pg_cryptohash_free.md) (src/common/cryptohash.c:243)
  - [pg_hmac_free](../p/pg_hmac_free.md) (src/common/hmac.c:295)
  - [freePGconn](../f/freePGconn.md) (src/interfaces/libpq/fe-connect.c:4665,4680)
  - [passwordFromFile](../p/passwordFromFile.md) (src/interfaces/libpq/fe-connect.c:7527,7554)

## Notes and Other Information
- The function is declared in src/include/port.h:428 and implemented as part of the PostgreSQL portability layer
- Used extensively throughout PostgreSQL for clearing sensitive cryptographic material and passwords
- The volatile function pointer technique used on non-Windows/non-memset_s systems is borrowed from OpenSSH
- This is a critical security function that helps prevent sensitive data from being recovered from memory dumps or swap files
- The function provides consistent behavior across all supported PostgreSQL platforms while using the most appropriate secure clearing method for each

## Simplified Source

```c
void
explicit_bzero(void *buf, size_t len)
{
    // Securely clear memory buffer to prevent data recovery
    // Uses platform-specific secure memory clearing function
    bzero_p(buf, len);
}
```