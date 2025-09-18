# backup_parse_compress_options

## Location
src/bin/pg_basebackup/pg_basebackup.c: 986 - 1013

## Overview
A parsing function that analyzes compression option strings provided via the -Z/--compress command-line option, separating location prefixes (client/server) from algorithm names and detail parameters.

## Definition
```c
static void backup_parse_compress_options(char *option, char **algorithm, char **detail, CompressionLocation *locationres)
```

## Detailed Description
This function performs the initial parsing stage of compression options specified via the --compress command-line argument in pg_basebackup. It specifically handles the parsing of optional location prefixes ("client-" or "server-") that specify where compression should be performed, then delegates the remaining parsing of algorithm names and detail parameters to the common `parse_compress_options` function.

The function is designed to be permissive at the parsing stage and does not validate the correctness of compression algorithms or parameters. For example, it will successfully parse nonsensical options like "client-turkey:sandwich" and leave validation for later stages. This design allows for flexible option processing while maintaining separation of concerns.

The function handles three compression location scenarios:
1. "server-" prefix: compression performed on the server side
2. "client-" prefix: compression performed on the client side  
3. No prefix: compression location unspecified

## Parameters / Member Variables
- `option`: Input string containing the full compression option to parse (may include location prefix)
- `algorithm`: Output parameter - pointer to string pointer that will receive the parsed algorithm name
- `detail`: Output parameter - pointer to string pointer that will receive the parsed detail/parameter string
- `locationres`: Output parameter - pointer to CompressionLocation enum that will receive the parsed compression location

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library function for string comparison)
  - [parse_compress_options](../p/parse_compress_options.md) (common compression option parsing function)
  - COMPRESS_LOCATION_SERVER (enumeration value for server-side compression)
  - COMPRESS_LOCATION_CLIENT (enumeration value for client-side compression)
  - COMPRESS_LOCATION_UNSPECIFIED (enumeration value for unspecified compression location)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_basebackup.c for processing --compress command-line options)

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- The function modifies the input `option` string by advancing the pointer past any recognized prefix
- Validation of compression algorithms and parameters is deliberately deferred to later processing stages
- The function supports both legacy integer-only compression specifications (defaulting to gzip) and modern algorithm:detail syntax
- The parsing is case-sensitive for the "client-" and "server-" prefixes
- This function works in conjunction with `parse_compress_options` to provide complete compression option parsing