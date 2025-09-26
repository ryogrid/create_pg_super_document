# pq_lockingcallback

## Location
src/interfaces/libpq/fe-secure-openssl.c: 738 - 768

## Overview
A callback function that provides mutex locking/unlocking operations for OpenSSL's internal thread safety mechanism in older OpenSSL versions (pre-1.1.0).

## Definition

```c
static void
pq_lockingcallback(int mode, int n, const char *file, int line)
```
## Detailed Description
This function serves as a callback for OpenSSL's legacy thread safety mechanisms. In OpenSSL versions prior to 1.1.0, applications were required to provide both thread identification and locking callbacks to ensure thread safety. This function specifically handles the locking requirement by performing mutex operations on a per-lock basis.

The function operates on a global array of mutexes () and uses the  parameter to determine whether to lock or unlock the mutex at index . When  is set in the mode, it acquires the mutex; otherwise, it releases it.

Error handling is minimal by design - if mutex operations fail, the function only asserts in debug builds and ignores errors in production, since there's no mechanism to report locking failures back to OpenSSL. This approach is considered acceptable since these callbacks are obsolete in modern OpenSSL versions.

## Parameters / Member Variables
- : Bitmask indicating the lock operation (CRYPTO_LOCK for lock, otherwise unlock)
- : Index of the mutex in the global lock array to operate on
- : Source file name where the lock operation was requested (unused)
- : Line number where the lock operation was requested (unused)

## Dependencies
- Functions called/Symbols referenced:
  - : Acquires a pthread mutex
  - : Releases a pthread mutex
- Called from (representative examples):
  - : Sets this function as OpenSSL's locking callback during SSL initialization
  - : Used during SSL system cleanup

## Notes and Other Information
- Return value: void (no return value)
- Only used with OpenSSL versions that require manual thread callbacks (< 1.1.0)
- Uses  for error checking in debug builds, ignores errors in production
- Operates on the global  mutex array
- The  and  parameters are provided by OpenSSL for debugging but are unused
- This is a static function internal to the OpenSSL secure connection implementation
- Located in 
- Part of the legacy threading support that becomes unnecessary with modern OpenSSL versions