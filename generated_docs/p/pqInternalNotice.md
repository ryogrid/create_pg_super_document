# pqInternalNotice

## Location
[src/interfaces/libpq/fe-exec.c:938-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L938-L992)

## Overview
pqInternalNotice is a utility function that produces internally-generated notice messages within libpq. It formats notice messages using a printf-style interface and delivers them through the configured notice hooks.

## Definition

```c
void
pqInternalNotice(const PGNoticeHooks *hooks, const char *fmt,...)
```
## Detailed Description
This function creates and sends notice messages that are generated internally by libpq, as opposed to notices received from the PostgreSQL server. It handles the complete process of message formatting, PGresult creation, field setup, and delivery to the registered notice receiver. The function automatically applies internationalization through libpq_gettext() and ensures proper message structure according to PostgreSQL's notice format.

The function creates a temporary PGresult with PGRES_NONFATAL_ERROR status to encapsulate the notice, populates it with the formatted message and appropriate severity fields, then passes it to the notice receiver callback before cleaning up.

## Parameters / Member Variables
- : Pointer to PGNoticeHooks structure containing the notice receiver callback and its argument
- : Printf-style format string for the notice message
- : Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  - vsnprintf
  - [libpq_gettext](../l/libpq_gettext.md)
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md)
  - [pqSaveMessageField](pqSaveMessageField.md)
  - [pqResultAlloc](pqResultAlloc.md)
  - [PQclear](../P/PQclear.md)
- Constants used:
  - PGRES_NONFATAL_ERROR
  - PG_DIAG_MESSAGE_PRIMARY
  - PG_DIAG_SEVERITY
  - PG_DIAG_SEVERITY_NONLOCALIZED
- Called from (representative examples):
  - [PQsetvalue](../P/PQsetvalue.md)
  - [check_field_number](../c/check_field_number.md)
  - [check_tuple_field_number](../c/check_tuple_field_number.md)
  - [check_param_number](../c/check_param_number.md)
  - [PQcmdTuples](../P/PQcmdTuples.md)
  - [pqGetInt](pqGetInt.md)
  - [pqPutInt](pqPutInt.md)

## Notes and Other Information
- The function uses a fixed 1024-byte buffer for message formatting, with safety measures to ensure null termination
- If memory allocation fails when creating the result message, it substitutes "out of memory" as a fallback
- The function checks if a notice receiver is registered before proceeding - if hooks->noticeRec is NULL, it returns early
- Messages are automatically internationalized using libpq_gettext()
- The function is designed for internal libpq use and creates notices that follow the same structure as server-generated notices
- The primary message should not include trailing newlines and should be single-line text