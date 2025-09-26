# ErrorData

## Location
src/include/utils/elog.h: 441 - 473

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
- : Error severity level (DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC)
- : Boolean flag controlling whether error is written to server log
- : Boolean flag controlling whether error is sent to client
- : When true, prevents STATEMENT: section from being included in error output
- : When true, prevents CONTEXT: section from being included in error output  
- : Source file name where ereport() was called (__FILE__ macro)
- : Line number in source file where ereport() was called (__LINE__ macro)
- : Function name where ereport() was called (__func__ macro)
- : Message translation domain for primary message
- : Message translation domain for context message
- : Encoded SQL error state code (SQLSTATE)
- : Primary error message, potentially translated
- : Detailed error explanation for both client and server
- : Detailed error explanation for server log only
- : Suggestion for resolving the error
- : Contextual information about where error occurred
- : Stack backtrace information for debugging
- : Original untranslated message identifier
- : Database schema name related to the error
- : Database table name related to the error
- : Database column name related to the error
- : Database data type name related to the error
- : Database constraint name related to the error
- : Character position in query string where error occurred
- : Character position in internal query where error occurred
- : Text of internally generated query that caused error
- : System errno value captured when error was detected
- : Memory context for managing associated string allocations

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextData
- Called from (representative examples):
  - errstart
  - errfinish  
  - errsave_start
  - errsave_finish
  - CopyErrorData
  - FreeErrorData
  - ThrowErrorData
  - ReThrowError
  - EmitErrorReport
  - write_csvlog
  - write_jsonlog
  - send_message_to_server_log
  - send_message_to_frontend

## Notes and Other Information
- Critical component of PostgreSQL's error handling infrastructure, used across all subsystems
- Memory management is crucial: all dynamic strings must be allocated in the associated memory context
- Supports both immediate error reporting and deferred error handling through save/restore mechanisms
- Internationalization support through message domains enables localized error messages
- Provides fine-grained control over error output routing (server logs vs. client messages)
- Database object name fields enable precise error location reporting for SQL operations
- The structure is designed to be safely copied and transferred between contexts during error propagation