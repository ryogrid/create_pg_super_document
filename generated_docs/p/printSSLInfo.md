# printSSLInfo

## Location
src/bin/psql/command.c: 3971 - 3998

## Overview
Displays detailed information about the current SSL/TLS connection if SSL encryption is active on the database connection.

## Definition


## Detailed Description
The  function provides users with comprehensive information about their SSL/TLS encrypted database connection. It queries various SSL attributes from the active connection and presents them in a user-friendly format, helping users verify their connection security.

Key behaviors include:
- **SSL connection detection**: First checks if SSL is actually in use before attempting to display information
- **Multi-attribute display**: Retrieves and displays protocol version, cipher suite, compression status, and ALPN (Application-Layer Protocol Negotiation) information
- **Graceful fallback**: Shows "unknown" for missing or unavailable SSL attributes rather than failing
- **Compression status formatting**: Converts compression status to simple "on"/"off" display for better user understanding
- **ALPN handling**: Shows "none" when ALPN is not negotiated or is empty
- **Localization support**: Uses translatable strings for user messages and fallback values

## Parameters / Member Variables
None - the function operates on the global pset.db connection

## Dependencies
- Functions called/Symbols referenced:
  - : Checks whether the connection is using SSL/TLS
  - : Retrieves specific SSL connection attributes (protocol, cipher, compression, alpn)
- Called from (representative examples):
  - : During connection establishment
  - : When displaying connection information via \conninfo command

## Notes and Other Information
- The function is designed to be non-intrusive - it silently returns if no SSL connection is active
- SSL attribute retrieval is done through libpq's SSL attribute interface, which abstracts the underlying SSL library details
- The compression display logic specifically handles the common case where compression is reported as "off"
- ALPN (Application-Layer Protocol Negotiation) support indicates HTTP/2 compatibility for protocols that support it
- All displayed text uses PostgreSQL's internationalization framework for proper localization
- The function provides essential security information that helps users verify their connection meets security requirements