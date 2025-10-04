# PQoidValue

## Location
[src/interfaces/libpq/fe-exec.c:3793-3821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3793-L3821)

## Overview
PQoidValue extracts the OID (Object Identifier) from INSERT command results and returns it as a proper Oid data type, providing a more type-safe alternative to PQoidStatus.

## Definition
```c
Oid PQoidValue(const PGresult *res)
```

## Detailed Description
PQoidValue serves as a more robust and type-safe version of PQoidStatus by parsing INSERT command status strings and returning the OID as PostgreSQL's native Oid data type rather than a string. The function performs comprehensive validation: it checks for NULL results, verifies the command status starts with "INSERT ", ensures the first character after "INSERT " is a digit, and uses strtoul to safely convert the numeric portion to an unsigned long. Additional validation ensures the parsing stopped at either a space or null terminator, indicating a clean conversion. If any validation fails, the function returns InvalidOid.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing query results from an INSERT operation

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (standard C library)
  - strtoul (standard C library)
  - InvalidOid (PostgreSQL constant)
- Called from (representative examples):
  - [PrintQueryStatus](PrintQueryStatus.md) (src/bin/psql/common.c:989)
  - [ecpg_process_output](../e/ecpg_process_output.md) (src/interfaces/ecpg/ecpglib/execute.c:1867)

## Notes and Other Information
- Returns InvalidOid for non-INSERT commands, NULL results, or parsing errors
- Performs more rigorous validation than PQoidStatus, including proper numeric conversion
- Uses strtoul for safe string-to-number conversion with error detection
- Validates that the OID portion contains only valid numeric characters
- The returned Oid type is more suitable for PostgreSQL internal operations than string representations
- Part of the libpq client interface for PostgreSQL database connectivity
- Preferred over PQoidStatus when OID values need to be used in further PostgreSQL operations

## Simplified Source

```c
Oid PQoidValue(const PGresult *res)
{
    char *endptr = NULL;
    unsigned long result;

    // Validate result and check for INSERT command format
    if (!res ||
        strncmp(res->cmdStatus, "INSERT ", 7) != 0 ||
        res->cmdStatus[7] < '0' ||
        res->cmdStatus[7] > '9')
        return InvalidOid;

    // Parse OID from command status string
    result = strtoul(res->cmdStatus + 7, &endptr, 10);

    // Validate successful parsing
    if (!endptr || (*endptr != ' ' && *endptr != '\0'))
        return InvalidOid;
    else
        return (Oid) result;
}
```