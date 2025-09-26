# is_ip_address

## Location
src/interfaces/libpq/fe-secure-openssl.c: 554 - 573

## Overview
A utility function that determines whether a given hostname string represents a valid IP address (either IPv4 or IPv6).

## Definition

```c
struct in_addr dummy4;
```
## Detailed Description
This function validates whether the provided hostname string is a valid IP address format. It supports both IPv4 and IPv6 address validation:

- For IPv4 addresses, it uses  to parse and validate the address format
- For IPv6 addresses (when  is defined), it uses  with  to validate the format

The function is primarily used in SSL/TLS certificate validation contexts where different validation rules apply to IP addresses versus domain names.

## Parameters / Member Variables
- : A null-terminated string containing the hostname or IP address to validate

## Dependencies
- Functions called/Symbols referenced:
  - : Used for IPv4 address validation
  - : Used for IPv6 address validation (when available)
- Called from (representative examples):
  - : Uses this function to determine certificate validation strategy

## Notes and Other Information
- This is a static function internal to the OpenSSL secure connection implementation
- IPv6 support is conditional on the  preprocessor definition
- The function uses dummy variables (, ) since it only needs to validate format, not store the parsed addresses
- Return value:  if the host string is a valid IP address,  otherwise
- Located in 