# PGNoticeHooks

## Location
[src/interfaces/libpq/libpq-int.h:159-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-int.h#L159-L160)

## Overview
PGNoticeHooks is a structure that encapsulates callback procedures for handling notice messages in the PostgreSQL libpq client library, providing a centralized way to process notices and notifications from the database server.

## Definition

```c
typedef struct PGEvent
{
	PGEventProc proc;			/* the function to call on events */
	char	   *name;			/* used only for error messages */
	void	   *passThrough;	/* pointer supplied at registration time */
	void	   *data;			/* optional state (instance) data */
	bool		resultInitialized;	/* T if RESULTCREATE/COPY succeeded */
} PGEvent;
```
## Detailed Description
PGNoticeHooks is a fundamental structure in PostgreSQL's libpq client library that manages notice message handling. It stores function pointers and associated arguments for two types of notice processing: a notice receiver that handles structured notice messages (PGresult objects), and a notice processor that handles simple string-based notice messages.

This structure is used in both PGconn (connection objects) and PGresult (result objects) to ensure that notice handling is consistent and properly propagated. When a PGresult is created, the notice hooks are copied from the originating PGconn so that operations on the result object can handle notices independently without requiring access to the connection object.

The structure supports two complementary notice handling mechanisms:
- **Notice Receiver**: Handles structured notice messages as PGresult objects, providing full access to notice fields and metadata
- **Notice Processor**: Handles simple string-based notice messages for basic text output

## Parameters / Member Variables
- `noticeRec`: Function pointer of type PQnoticeReceiver that receives structured notice messages as PGresult objects
- `noticeRecArg`: Void pointer passed as the first argument to the notice receiver function, allowing client code to pass context data
- `noticeProc`: Function pointer of type PQnoticeProcessor that receives simple string-based notice messages
- `noticeProcArg`: Void pointer passed as the first argument to the notice processor function, allowing client code to pass context data

## Dependencies
- Functions called/Symbols referenced:
  - PQnoticeReceiver (function pointer type)
  - PQnoticeProcessor (function pointer type)
- Called from (representative examples):
  - [pqInternalNotice](../p/pqInternalNotice.md) (processes notices using the hooks)
  - [pg_result](../p/pg_result.md) struct (contains noticeHooks member)
  - pg_conn struct (contains noticeHooks member)

## Notes and Other Information
- The structure is defined in src/interfaces/libpq/libpq-int.h:153-159
- Notice hooks are copied from PGconn to PGresult objects to enable independent notice processing
- If noticeRec is NULL, notice processing may be skipped or handled differently
- This structure enables flexible notice handling by allowing applications to register custom callback functions
- The hooks support both structured (PGresult-based) and simple (string-based) notice processing mechanisms
- Used internally by libpq functions like pqInternalNotice for consistent notice message handling across the library