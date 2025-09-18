# PQinitSSL

## Location
src/interfaces/libpq/fe-secure.c: 115 - 126

## Overview
Allows client applications to control SSL library initialization by indicating whether OpenSSL has already been initialized.

## Definition
```c
void PQinitSSL(int do_init)
```

## Detailed Description
PQinitSSL is an exported function that provides applications with control over SSL library initialization. When SSL support is compiled in (USE_SSL), this function calls the underlying pgtls_init_library function to initialize both SSL and crypto libraries based on the provided flag. This is particularly useful for applications that have already initialized OpenSSL themselves and want to prevent libpq from re-initializing it, which could cause conflicts.

## Parameters / Member Variables
- `do_init`: Integer flag indicating whether to initialize SSL libraries (non-zero to initialize, 0 to skip initialization)

## Dependencies
- Functions called/Symbols referenced:
  - [pgtls_init_library](../p/pgtls_init_library.md)
  - USE_SSL (conditional compilation flag)
- Called from (representative examples):
  - Referenced in PQsetdb header (src/interfaces/libpq/libpq-fe.h:418)

## Notes and Other Information
- Only has effect when PostgreSQL is compiled with SSL support (USE_SSL defined)
- When USE_SSL is not defined, this function becomes a no-op
- Calls pgtls_init_library with the same value for both SSL and crypto initialization
- Applications should call this before making any PostgreSQL connections if they want to control SSL initialization
- Typically called with 0 by applications that have already initialized OpenSSL to prevent double initialization