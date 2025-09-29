# ECPGnoticeReceiver

## Location
[src/interfaces/ecpg/ecpglib/connect.c:208-259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/connect.c#L208-L259)

## Overview
ECPGnoticeReceiver is a static callback function that processes PostgreSQL notice and warning messages in ECPG, translating them into SQLCA (SQL Communications Area) format for application consumption.

## Definition
```c
static void ECPGnoticeReceiver(void *arg, const PGresult *result)
```

## Detailed Description
ECPGnoticeReceiver serves as a notice processor callback for PostgreSQL connections in ECPG applications. When PostgreSQL generates notices or warnings, this function extracts the SQLSTATE and message from the result, maps them to appropriate SQLCODE values for backward compatibility, and populates the SQLCA structure. It filters out informational messages (SQLSTATE starting with "00") and only processes actual warnings. The function performs SQLSTATE-to-SQLCODE translation to maintain compatibility with older SQL standards while providing modern SQLSTATE information.

## Parameters / Member Variables
- `arg`: Unused parameter (kept for callback signature compatibility)
- `result`: PostgreSQL result object containing the notice/warning information including SQLSTATE and message text

## Dependencies
- Functions called/Symbols referenced:
  - [PQresultErrorField](../P/PQresultErrorField.md)
  - ECPGget_sqlca
  - [ecpg_log](../e/ecpg_log.md)
  - [ecpg_gettext](../e/ecpg_gettext.md)
  - strncmp/strcmp/strlen
- Called from (representative examples):
  - [ECPGconnect](ECPGconnect.md) (registered as notice receiver)

## Notes and Other Information
- Static function, only accessible within the connect.c file
- Implements the PQnoticeReceiver callback interface
- Maps specific SQLSTATEs to legacy SQLCODE values for backward compatibility
- Populates SQLCA warning fields (sqlwarn[0] and sqlwarn[2] set to "W")
- Filters out success conditions (SQLSTATE 00xxx) which are not warnings
- Ensures message text is null-terminated and fits within SQLCA limits
- Part of ECPGs error and warning handling infrastructure

## Simplified Source

```c
static void ECPGnoticeReceiver(void *arg, const PGresult *result) {
    char *sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
    char *message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
    struct sqlca_t *sqlca = ECPGget_sqlca();
    int sqlcode;

    // Handle memory allocation failure
    if (!sqlca) {
        ecpg_log("out of memory");
        return;
    }

    // Set default values for missing fields
    if (!sqlstate)
        sqlstate = ECPG_SQLSTATE_ECPG_INTERNAL_ERROR;
    if (!message)
        message = ecpg_gettext("empty message text");

    // Skip success conditions (not warnings)
    if (strncmp(sqlstate, "00", 2) == 0)
        return;

    ecpg_log("ECPGnoticeReceiver: %s\n", message);

    // Map SQLSTATE to legacy SQLCODE for backward compatibility
    if (strcmp(sqlstate, ECPG_SQLSTATE_INVALID_CURSOR_NAME) == 0)
        sqlcode = ECPG_WARNING_UNKNOWN_PORTAL;
    else if (strcmp(sqlstate, ECPG_SQLSTATE_ACTIVE_SQL_TRANSACTION) == 0)
        sqlcode = ECPG_WARNING_IN_TRANSACTION;
    else if (strcmp(sqlstate, ECPG_SQLSTATE_NO_ACTIVE_SQL_TRANSACTION) == 0)
        sqlcode = ECPG_WARNING_NO_TRANSACTION;
    else if (strcmp(sqlstate, ECPG_SQLSTATE_DUPLICATE_CURSOR) == 0)
        sqlcode = ECPG_WARNING_PORTAL_EXISTS;
    else
        sqlcode = 0;

    // Populate SQLCA structure with warning information
    strncpy(sqlca->sqlstate, sqlstate, sizeof(sqlca->sqlstate));
    sqlca->sqlcode = sqlcode;
    sqlca->sqlwarn[2] = 'W';
    sqlca->sqlwarn[0] = 'W';

    // Copy message text with safe truncation
    strncpy(sqlca->sqlerrm.sqlerrmc, message, sizeof(sqlca->sqlerrm.sqlerrmc));
    sqlca->sqlerrm.sqlerrmc[sizeof(sqlca->sqlerrm.sqlerrmc) - 1] = 0;
    sqlca->sqlerrm.sqlerrml = strlen(sqlca->sqlerrm.sqlerrmc);

    ecpg_log("raising sqlcode %d\n", sqlcode);
}
```