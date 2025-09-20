# ErrorSaveContext

## Location
[src/include/nodes/miscnodes.h:43-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/miscnodes.h#L43-L49)

## Overview
ErrorSaveContext is a function call context node used for handling "soft" errors in PostgreSQL, allowing callers to trap and handle errors gracefully without triggering a full error abort.

## Definition

```c
typedef struct ErrorSaveContext
{
	NodeTag		type;
	bool		error_occurred; /* set to true if we detect a soft error */
	bool		details_wanted; /* does caller want more info than that? */
	ErrorData  *error_data;		/* details of error, if so */
} ErrorSaveContext;
```
## Detailed Description
ErrorSaveContext provides a mechanism for PostgreSQL functions to handle errors in a "soft" manner, where errors can be caught and handled locally rather than propagating up the call stack as exceptions. This is particularly useful for functions that need to validate input or perform operations that might fail in expected ways.

The context is initialized by a caller with all fields set to zero/NULL except for the NodeTag. It can be passed to SQL-callable functions via the FunctionCallInfo.context field, or directly to subroutines below the SQL call level.

When an error occurs, the error_occurred flag is set to true. If details_wanted was set to true by the caller, the error_data field is populated with detailed error information stored in the callee's memory context. The SOFT_ERROR_OCCURRED macro provides a convenient way to check if a soft error was reported.

## Parameters / Member Variables
- : NodeTag identifier for the structure type
- : Boolean flag set to true when a soft error is detected
- : Boolean flag indicating whether the caller wants detailed error information beyond just knowing an error occurred
- : Pointer to ErrorData structure containing detailed error information (only populated if details_wanted is true)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [ExecInitJsonExpr](ExecInitJsonExpr.md)
  - [ExecEvalCoerceViaIOSafe](ExecEvalCoerceViaIOSafe.md)
  - [ExecEvalJsonCoercion](ExecEvalJsonCoercion.md)
  - [make_const](../m/make_const.md)
  - [parseTypeString](../p/parseTypeString.md)
  - [jsonb_populate_record_valid](../j/jsonb_populate_record_valid.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md)
  - [executeDateTimeMethod](../e/executeDateTimeMethod.md)
  - [pg_input_is_valid](../p/pg_input_is_valid.md)
  - [pg_input_error_info](../p/pg_input_error_info.md)
  - [to_regproc](../t/to_regproc.md) family functions
  - [parse_tsquery](../p/parse_tsquery.md)
  - [xml_is_document](../x/xml_is_document.md)
  - [errsave_start](../e/errsave_start.md)
  - [errsave_finish](../e/errsave_finish.md)

## Notes and Other Information
- The structure is defined in src/include/nodes/miscnodes.h:43-49
- A convenient macro SOFT_ERROR_OCCURRED(escontext) is provided to check if a soft error occurred
- Error details are stored in the callee's memory context, and FreeErrorData() can be called to release them (though this is typically not necessary if the called code runs in a short-lived context)
- This mechanism is widely used throughout PostgreSQL for input validation, type conversion, JSON processing, XML processing, and various utility functions where graceful error handling is preferred over exception-based error propagation
- The soft error mechanism is particularly important for functions that are expected to handle invalid input gracefully, such as input validation functions and data conversion routines