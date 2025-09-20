# PQgetSSLKeyPassHook_OpenSSL

## Location
[src/interfaces/libpq/fe-secure.c:476-481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure.c#L476-L481)

## Overview
Returns the currently installed SSL private key passphrase callback function for OpenSSL connections.

## Definition


Where  is defined as:


## Detailed Description
PQgetSSLKeyPassHook_OpenSSL retrieves the currently installed callback function that is used to obtain passphrases for encrypted SSL private key files. This function returns a pointer to the callback function that was previously set using PQsetSSLKeyPassHook_OpenSSL, or NULL if no custom hook has been installed.

The returned function pointer, when called, should fill the provided buffer with the passphrase for the SSL private key file and return the length of the passphrase. This mechanism allows applications to provide custom passphrase input methods (such as prompting the user, reading from a secure store, etc.) instead of relying on OpenSSL's default passphrase callback.

## Parameters / Member Variables


## Return Value
- Returns a function pointer of type 
- Returns NULL if no custom passphrase hook has been set
- The returned function pointer has the signature: 

## Callback Function Parameters
When the returned function is called, it receives:
- : Buffer to store the passphrase
- : Maximum size of the buffer
- : The PostgreSQL connection context

## Dependencies
- Functions called/Symbols referenced:
  - PQsslKeyPassHook (static variable access)
- Called from (representative examples):
  - Referenced in libpq-fe.h header

## Notes and Other Information
- This function is part of the OpenSSL-specific SSL key management API
- The returned function pointer should not be freed by the caller
- Applications can use this to save and restore SSL key passphrase hooks
- The callback function should return the length of the passphrase placed in the buffer
- This is a getter function paired with PQsetSSLKeyPassHook_OpenSSL for setting the hook
- Thread safety depends on the OpenSSL version and how the callback is implemented
- The passphrase callback is global to all connections using the same libpq instance