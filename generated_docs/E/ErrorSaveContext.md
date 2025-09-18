# ErrorSaveContext

## Location
src/include/nodes/miscnodes.h: 43 - 49

## Overview
ErrorSaveContext is a function call context node used for handling "soft" errors in PostgreSQL, allowing callers to trap and handle errors gracefully without triggering a full error abort.

## Definition


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
  - BeginCopyFrom
  - ExecInitJsonExpr
  - ExecEvalCoerceViaIOSafe
  - ExecEvalJsonCoercion
  - make_const
  - parseTypeString
  - jsonb_populate_record_valid
  - executeItemOptUnwrapTarget
  - executeDateTimeMethod
  - pg_input_is_valid
  - pg_input_error_info
  - to_regproc family functions
  - parse_tsquery
  - xml_is_document
  - errsave_start
  - errsave_finish

## Notes and Other Information
- The structure is defined in src/include/nodes/miscnodes.h:43-49
- A convenient macro SOFT_ERROR_OCCURRED(escontext) is provided to check if a soft error occurred
- Error details are stored in the callee's memory context, and FreeErrorData() can be called to release them (though this is typically not necessary if the called code runs in a short-lived context)
- This mechanism is widely used throughout PostgreSQL for input validation, type conversion, JSON processing, XML processing, and various utility functions where graceful error handling is preferred over exception-based error propagation
- The soft error mechanism is particularly important for functions that are expected to handle invalid input gracefully, such as input validation functions and data conversion routines