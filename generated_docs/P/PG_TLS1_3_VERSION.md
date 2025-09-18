# PG_TLS1_3_VERSION

## Location
src/include/libpq/libpq.h: 132 - 143

## Overview
PG_TLS1_3_VERSION is an enumeration constant that represents TLS version 1.3 in PostgreSQL's SSL/TLS protocol version configuration system.

## Definition


## Detailed Description
PG_TLS1_3_VERSION is part of PostgreSQL's SSL/TLS configuration infrastructure that allows administrators to specify minimum and maximum TLS protocol versions for secure connections. This enumeration value specifically represents TLS version 1.3, which is the most recent and secure version of the TLS protocol.

The constant is used in PostgreSQL's configuration system to map user-friendly string representations ("TLSv1.3") to internal numeric values. It serves as an intermediate representation between the configuration parsing layer and the OpenSSL library integration.

## Parameters / Member Variables
This is an enumeration constant with no parameters or member variables. Its numeric value is 4 (following the sequence starting from PG_TLS_ANY = 0).

## Dependencies
- Functions called/Symbols referenced:
  - ssl_protocol_version_to_openssl (converts to OpenSSL TLS1_3_VERSION)
  - ssl_protocol_version_to_string (converts to "TLSv1.3" string)
- Called from (representative examples):
  - ssl_protocol_versions_info (configuration enum table)
  - ssl_min_protocol_version/ssl_max_protocol_version configuration variables
  - SSL protocol version validation and conversion routines

## Notes and Other Information
- Used in PostgreSQL's GUC (Grand Unified Configuration) system for ssl_min_protocol_version and ssl_max_protocol_version parameters
- Maps to OpenSSL's TLS1_3_VERSION constant when available (compile-time conditional)
- TLS 1.3 provides enhanced security features including forward secrecy and reduced handshake overhead
- The actual availability of TLS 1.3 depends on the underlying OpenSSL library version and compilation flags
- Part of a static assertion that ensures the ssl_protocol_versions_info array length matches the number of enum values
- Configuration string "TLSv1.3" maps to this enumeration value in guc_tables.c