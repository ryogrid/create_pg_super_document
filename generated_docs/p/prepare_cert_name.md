# prepare_cert_name

## Location
src/backend/libpq/be-secure-openssl.c: 1153 - 1163

## Overview
The `prepare_cert_name` function processes SSL certificate names to ensure they are safe for logging by truncating overly long names and escaping unprintable ASCII characters.

## Definition
```c
static char *
prepare_cert_name(char *name)
```

## Detailed Description
This static utility function examines a certificate name (typically from SSL certificate subject or issuer fields) and prepares it for safe logging. The function addresses two primary concerns:

1. **Length limitation**: Certificate names that exceed 71 characters are truncated to a reasonable length for log output
2. **Character safety**: Unprintable ASCII characters are escaped to prevent log corruption or security issues

The function implements a smart truncation strategy that preserves the end of the name rather than the beginning, since the most specific certificate fields (like Common Name) typically appear at the end and provide the most useful information to users.

The 71-character limit is designed to accommodate the longest possible Common Name (64 characters) plus a reasonable prefix (7 characters for something like ".../CN="), making the output both informative and readable in logs.

## Parameters / Member Variables
- `name`: Input certificate name string to be processed (char pointer). This string may be modified in place during processing for implementation efficiency.

## Dependencies
- Functions called/Symbols referenced:
  - `strlen`: Standard C library function to get string length
  - `pg_clean_ascii`: PostgreSQL utility function that escapes unprintable ASCII characters
- Called from (representative examples):
  - [verify_cb](../v/verify_cb.md): SSL certificate verification callback function (lines 1234, 1238 in be-secure-openssl.c)

## Notes and Other Information
- The function is declared as `static`, limiting its scope to the be-secure-openssl.c file
- Returns a newly allocated string (via `pg_clean_ascii`) that must be freed by the caller
- Modifies the input string in place for truncation, then passes it to `pg_clean_ascii` for character escaping
- The MAXLEN constant (71) is defined and undefined locally within the function scope
- When truncating, the first three characters of the truncated portion are replaced with "..." to indicate truncation
- Part of PostgreSQL's SSL/TLS security infrastructure, specifically used during certificate verification for logging purposes