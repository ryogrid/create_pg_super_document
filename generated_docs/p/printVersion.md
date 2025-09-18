printVersion

## Overview
Displays the pgbench version banner along with PostgreSQL server version information when there is a version mismatch.

## Definition
static void printVersion(PGconn *con)

## Detailed Description
The printVersion function prints a version banner for pgbench that includes both the client (pgbench) version and the PostgreSQL server version when they differ. It first retrieves the server version using PostgreSQL connection functions and compares it to the client version. If versions match, it displays only the pgbench version. If they differ, it displays both versions to alert users to potential compatibility issues. The function attempts to get the full server version string including development indicators before falling back to formatted version numbers.

## Parameters / Member Variables
- con: Active PostgreSQL database connection used to query server version information

## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md) - Gets numeric server version from PostgreSQL connection
  - PQparameterStatus - Retrieves server parameter values including version string
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md) - Formats numeric version into readable string
  - printf - Output formatting function
  - fflush - Forces output buffer flush
  - PG_VERSION_NUM - Compile-time client version number
  - PG_VERSION - Compile-time client version string
- Called from (representative examples):
  - [main](../m/main.md) - Called during pgbench startup to display version information

## Notes and Other Information
- Only displays server version when it differs from client version to highlight potential compatibility issues
- Attempts to retrieve full server version text which may include development indicators like "devel"
- Falls back to formatted numeric version if full version string is unavailable
- Uses internationalization macros (_()) for translatable strings
- Flushes stdout to ensure immediate display of version information
- Part of pgbench startup sequence for version verification and user information