# ErrorData

## Location
[src/include/utils/elog.h:441-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/elog.h#L441-L473)

## Overview
ErrorData is a structure that holds all data accumulated during any one ereport() cycle in PostgreSQL, serving as the central container for error information including messages, metadata, and context for error reporting and logging.

## Definition

```c
typedef struct ErrorData
{
	int			elevel;			/* error level */
	bool		output_to_server;	/* will report to server log? */
	bool		output_to_client;	/* will report to client? */
	bool		hide_stmt;		/* true to prevent STATEMENT: inclusion */
	bool		hide_ctx;		/* true to prevent CONTEXT: inclusion */
	const char *filename;		/* __FILE__ of ereport() call */
	int			lineno;			/* __LINE__ of ereport() call */
	const char *funcname;		/* __func__ of ereport() call */
	const char *domain;			/* message domain */
	const char *context_domain; /* message domain for context message */
	int			sqlerrcode;		/* encoded ERRSTATE */
	char	   *message;		/* primary error message (translated) */
	char	   *detail;			/* detail error message */
	char	   *detail_log;		/* detail error message for server log only */
	char	   *hint;			/* hint message */
	char	   *context;		/* context message */
	char	   *backtrace;		/* backtrace */
	const char *message_id;		/* primary message's id (original string) */
	char	   *schema_name;	/* name of schema */
	char	   *table_name;		/* name of table */
	char	   *column_name;	/* name of column */
	char	   *datatype_name;	/* name of datatype */
	char	   *constraint_name;	/* name of constraint */
	int			cursorpos;		/* cursor index into query string */
	int			internalpos;	/* cursor index into internalquery */
	char	   *internalquery;	/* text of internally-generated query */
	int			saved_errno;	/* errno at entry */

	/* context containing associated non-constant strings */
	struct MemoryContextData *assoc_context;
} ErrorData;
```
## Detailed Description
The ErrorData structure is PostgreSQL's comprehensive error data container used throughout the error reporting system. It accumulates all relevant information during an ereport() cycle, including error levels, messages, source location, database object names, and control flags for output routing.

The structure supports internationalization through message domains and provides detailed context information for debugging. It maintains both client-facing and server log-specific versions of error details, allowing different levels of information disclosure. The structure also handles memory management through an associated memory context for dynamic string allocations.

All non-NULL string pointers (except const ones) must point to palloc'd data, with the associated memory context tracking allocations for proper cleanup. This design ensures memory safety during error handling scenarios.

## Parameters / Member Variables
- `elevel`: Error severity level (DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC)
- `output_to_server`: Boolean flag controlling whether error is written to server log
- `output_to_client`: Boolean flag controlling whether error is sent to client
- `hide_stmt`: When true, prevents STATEMENT: section from being included in error output
- `hide_ctx`: When true, prevents CONTEXT: section from being included in error output
- `*filename`: Source file name where ereport() was called (__FILE__ macro)
- `lineno`: Line number in source file where ereport() was called (__LINE__ macro)
- `*funcname`: Function name where ereport() was called (__func__ macro)
- `*domain`: Message translation domain for primary message
- `*context_domain`: Message translation domain for context message
- `sqlerrcode`: Encoded SQL error state code (SQLSTATE)
- `*message`: Primary error message, potentially translated
- `*detail`: Detailed error explanation for both client and server
- `*detail_log`: Detailed error explanation for server log only
- `*hint`: Suggestion for resolving the error
- `*context`: Contextual information about where error occurred
- `*backtrace`: Stack backtrace information for debugging
- `*message_id`: Original untranslated message identifier
- `*schema_name`: Database schema name related to the error
- `*table_name`: Database table name related to the error
- `*column_name`: Database column name related to the error
- `*datatype_name`: Database data type name related to the error
- `*constraint_name`: Database constraint name related to the error
- `cursorpos`: Character position in query string where error occurred
- `internalpos`: Character position in internal query where error occurred
- `*internalquery`: Text of internally generated query that caused error
- `saved_errno`: System errno value captured when error was detected
- `*assoc_context`: Memory context for managing associated string allocations
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextData](../M/MemoryContextData.md)
- Called from (representative examples):
  - [errstart](../e/errstart.md)
  - [errfinish](../e/errfinish.md)  
  - [errsave_start](../e/errsave_start.md)
  - [errsave_finish](../e/errsave_finish.md)
  - [CopyErrorData](../C/CopyErrorData.md)
  - [FreeErrorData](../F/FreeErrorData.md)
  - [ThrowErrorData](../T/ThrowErrorData.md)
  - [ReThrowError](../R/ReThrowError.md)
  - [EmitErrorReport](EmitErrorReport.md)
  - [write_csvlog](../w/write_csvlog.md)
  - [write_jsonlog](../w/write_jsonlog.md)
  - [send_message_to_server_log](../s/send_message_to_server_log.md)
  - [send_message_to_frontend](../s/send_message_to_frontend.md)

## Notes and Other Information
- Critical component of PostgreSQL's error handling infrastructure, used across all subsystems
- Memory management is crucial: all dynamic strings must be allocated in the associated memory context
- Supports both immediate error reporting and deferred error handling through save/restore mechanisms
- Internationalization support through message domains enables localized error messages
- Provides fine-grained control over error output routing (server logs vs. client messages)
- Database object name fields enable precise error location reporting for SQL operations
- The structure is designed to be safely copied and transferred between contexts during error propagation