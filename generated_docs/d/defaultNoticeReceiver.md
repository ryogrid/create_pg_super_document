# defaultNoticeReceiver

## Location
[src/interfaces/libpq/fe-connect.c:7361-7375](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7361-L7375)

## Overview
The default notice message receiver function that processes PostgreSQL notice messages by extracting the standard notice text and forwarding it to the configured notice processor.

## Definition
```c
static void defaultNoticeReceiver(void *arg, const PGresult *res)
```

## Detailed Description
This function serves as the default implementation for handling notice messages in libpq. It acts as an intermediary layer between PostgreSQL's notice generation system and the user-defined notice processor. The function extracts the notice message text from a PGresult structure and passes it to the registered notice processor function.

The implementation follows a two-level setup that exists primarily for backwards compatibility with older PostgreSQL client applications. The first level (this function) receives the full PGresult containing the notice, while the second level (the notice processor) receives just the text message.

## Parameters / Member Variables
- `arg`: A void pointer argument that is not used in this implementation (marked as unused)
- `res`: A pointer to the PGresult structure containing the notice message and associated hooks

## Dependencies
- Functions called/Symbols referenced:
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
- Called from (representative examples):
  - internalPQconninfoOption (fe-connect.c:436)
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md) (fe-connect.c:4571)

## Notes and Other Information
- This function is marked as static, indicating it's only used within the fe-connect.c file
- The two-level notice handling setup (receiver → processor) is maintained for backwards compatibility
- The comment suggests that the use of PQsetNoticeProcessor might be deprecated in future versions
- The function checks if a notice processor is registered before attempting to call it
- This is part of libpq's client-side PostgreSQL interface library