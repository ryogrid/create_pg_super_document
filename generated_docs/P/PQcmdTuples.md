# PQcmdTuples

## Location
[src/interfaces/libpq/fe-exec.c:3822-3875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3822-L3875)

## Overview
PQcmdTuples extracts the number of affected tuples from SQL command results, supporting INSERT, UPDATE, DELETE, MERGE, MOVE, FETCH, COPY, and SELECT operations.

## Definition
```c
char *PQcmdTuples(PGresult *res)
```

## Detailed Description
PQcmdTuples parses the command status string from a PGresult to extract the count of tuples affected by various SQL operations. The function handles different command formats: for INSERT commands, it skips over the OID portion to find the tuple count; for SELECT, DELETE, and UPDATE, it directly accesses the count after the 7-character command prefix; for FETCH and MERGE, it uses a 6-character prefix; and for MOVE and COPY, it uses a 5-character prefix. The function validates that the extracted portion contains only digits and reports parsing errors through the internal notice system if the command status format is unexpected.

## Parameters / Member Variables
- `res`: Pointer to a PGresult structure containing query results

## Dependencies
- Functions called/Symbols referenced:
  - [pqInternalNotice](../p/pqInternalNotice.md)
  - strncmp (standard C library)
  - isdigit (standard C library)
- Called from (representative examples):
  - [SetResultVariables](../S/SetResultVariables.md) (src/bin/psql/common.c:465)
  - [ecpg_process_output](../e/ecpg_process_output.md) (src/interfaces/ecpg/ecpglib/execute.c:1868)

## Notes and Other Information
- Returns an empty string ("") for unsupported command types or NULL results
- For INSERT commands, automatically skips the OID portion to find the tuple count
- Performs validation to ensure the extracted string contains only numeric digits
- Uses pqInternalNotice to report parsing errors when command status format is unexpected
- Supports a wide range of SQL commands: INSERT, UPDATE, DELETE, MERGE, MOVE, FETCH, COPY, SELECT
- The returned pointer points directly into the command status string, no separate allocation
- Comment in code suggests this should return an int rather than a string for better type safety
- Part of the libpq client interface for PostgreSQL database connectivity

## Simplified Source

```c
char *PQcmdTuples(PGresult *res)
{
    char *p, *c;

    if (!res)
        return "";

    // Parse different command types to find tuple count
    if (strncmp(res->cmdStatus, "INSERT ", 7) == 0) {
        p = res->cmdStatus + 7;
        // Skip OID and space for INSERT
        while (*p && *p != ' ')
            p++;
        if (*p == 0)
            goto interpret_error;
        p++;
    }
    else if (strncmp(res->cmdStatus, "SELECT ", 7) == 0 ||
             strncmp(res->cmdStatus, "DELETE ", 7) == 0 ||
             strncmp(res->cmdStatus, "UPDATE ", 7) == 0)
        p = res->cmdStatus + 7;
    else if (strncmp(res->cmdStatus, "FETCH ", 6) == 0 ||
             strncmp(res->cmdStatus, "MERGE ", 6) == 0)
        p = res->cmdStatus + 6;
    else if (strncmp(res->cmdStatus, "MOVE ", 5) == 0 ||
             strncmp(res->cmdStatus, "COPY ", 5) == 0)
        p = res->cmdStatus + 5;
    else
        return "";

    // Validate that we have a valid integer
    for (c = p; *c; c++) {
        if (!isdigit((unsigned char) *c))
            goto interpret_error;
    }
    if (c == p)
        goto interpret_error;

    return p;

interpret_error:
    pqInternalNotice(&res->noticeHooks,
                     "could not interpret result from server: %s",
                     res->cmdStatus);
    return "";
}
```