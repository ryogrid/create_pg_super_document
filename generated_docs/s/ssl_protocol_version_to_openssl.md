# ssl_protocol_version_to_openssl

## Location
[src/backend/libpq/be-secure-openssl.c:1691-1725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-openssl.c#L1691-L1725)

## Overview
Converts PostgreSQL TLS protocol version GUC enumeration values to their corresponding OpenSSL constant values.

## Definition
```c
static int ssl_protocol_version_to_openssl(int v)
```

## Detailed Description
This static function provides a mapping layer between PostgreSQL's internal TLS protocol version enumeration (used in GUC parameters like ssl_min_protocol_version and ssl_max_protocol_version) and OpenSSL's protocol version constants. The function ensures that PostgreSQL's TLS configuration is independent of OpenSSL availability and version while still being able to configure the underlying OpenSSL library properly.

The function includes conditional compilation directives to handle different OpenSSL versions that may not support certain TLS protocol versions, returning -1 for unsupported versions and allowing calling code to detect and handle unsupported protocols gracefully.

## Parameters / Member Variables
- `v`: PostgreSQL internal TLS protocol version enumeration value to convert

## Dependencies
- Functions called/Symbols referenced:
  - PG_TLS_ANY (PostgreSQL constant for any TLS version)
  - PG_TLS1_VERSION (PostgreSQL constant for TLS 1.0)
  - PG_TLS1_1_VERSION (PostgreSQL constant for TLS 1.1)
  - PG_TLS1_2_VERSION (PostgreSQL constant for TLS 1.2)
  - [PG_TLS1_3_VERSION](../P/PG_TLS1_3_VERSION.md) (PostgreSQL constant for TLS 1.3)
  - TLS1_VERSION (OpenSSL constant for TLS 1.0)
  - TLS1_1_VERSION (OpenSSL constant for TLS 1.1, if available)
  - TLS1_2_VERSION (OpenSSL constant for TLS 1.2, if available)
  - TLS1_3_VERSION (OpenSSL constant for TLS 1.3, if available)
- Called from (representative examples):
  - [be_tls_init](../b/be_tls_init.md) (backend TLS initialization)
  - [initialize_SSL](../i/initialize_SSL.md) (libpq SSL initialization)

## Notes and Other Information
- Static function - only accessible within the be-secure-openssl.c compilation unit
- Returns 0 for PG_TLS_ANY to indicate no specific version restriction
- Returns -1 for unsupported or unrecognized protocol versions
- Uses conditional compilation to handle different OpenSSL versions gracefully
- Mirrors similar functionality in libpq's fe-secure-openssl.c for consistency
- Essential for configuring OpenSSL context with user-specified minimum and maximum TLS versions
- Allows PostgreSQL to maintain protocol version independence from specific OpenSSL installations
- Part of the GUC (Grand Unified Configuration) system integration for TLS settings

## Simplified Source

```c
// Simplified version of ssl_protocol_version_to_openssl
static int ssl_protocol_version_to_openssl(int v) {
    // Handle the "any version" case
    if (v == PG_TLS_ANY) {
        return 0;  // No version restriction
    }

    // Map PostgreSQL TLS version constants to OpenSSL constants
    switch (v) {
        case PG_TLS1_VERSION:
            return TLS1_VERSION;

        case PG_TLS1_1_VERSION:
            // Only available if OpenSSL supports TLS 1.1
            return TLS1_1_VERSION;

        case PG_TLS1_2_VERSION:
            // Only available if OpenSSL supports TLS 1.2
            return TLS1_2_VERSION;

        case PG_TLS1_3_VERSION:
            // Only available if OpenSSL supports TLS 1.3
            return TLS1_3_VERSION;

        default:
            // Unsupported or unrecognized version
            return -1;
    }
}
```

Key simplifications made:
- Consolidated the switch statement logic for clearer flow
- Removed conditional compilation directives for readability
- Added descriptive comments for each major step
- Simplified the control flow while preserving the mapping logic
- Maintained the essential return values (0 for any, -1 for unsupported)