# PQoidStatus

## Location
[src/interfaces/libpq/fe-exec.c:3765-3792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3765-L3792)

## Overview
PQoidStatus extracts the OID (Object Identifier) string from INSERT command results, returning an empty string for all other command types.

## Definition
```c
char *PQoidStatus(const PGresult *res)
```

## Detailed Description
PQoidStatus is specifically designed to extract OID information from INSERT command results. When PostgreSQL executes an INSERT statement on a table with OIDs, the command status includes the OID of the newly inserted row in the format "INSERT oid count". This function parses the command status string, checks if it starts with "INSERT ", and if so, extracts the numeric OID portion. For any other command type or invalid results, it returns an empty string. The function uses a static buffer to store the extracted OID string.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing query results from an INSERT operation

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library)
  - strspn (standard C library)
  - memcpy (standard C library)
- Called from (representative examples):
  - (Limited usage found in indexed codebase)

## Notes and Other Information
- Returns an empty string ("") for non-INSERT commands or NULL results
- Uses a static buffer of 24 characters to hold the OID string
- The function specifically looks for command status strings starting with "INSERT "
- OID extraction is limited to numeric characters following the "INSERT " prefix
- Buffer overflow protection ensures the extracted OID doesn't exceed buffer size
- Part of the libpq client interface for PostgreSQL database connectivity
- OIDs are largely deprecated in modern PostgreSQL versions, making this function less commonly used

## Simplified Source

```c
char *PQoidStatus(const PGresult *res) {
    // Static buffer to hold the OID string
    static char buf[24];
    size_t len;

    // Return empty string for NULL result or non-INSERT commands
    if (!res || strncmp(res->cmdStatus, "INSERT ", 7) != 0)
        return "";

    // Extract numeric OID portion after "INSERT "
    len = strspn(res->cmdStatus + 7, "0123456789");
    if (len > sizeof(buf) - 1)
        len = sizeof(buf) - 1;

    // Copy OID digits to buffer and null-terminate
    memcpy(buf, res->cmdStatus + 7, len);
    buf[len] = '\0';

    return buf;
}
```