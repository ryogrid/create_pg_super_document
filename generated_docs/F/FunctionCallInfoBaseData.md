# FunctionCallInfoBaseData

## Location
[src/include/fmgr.h:85-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fmgr.h#L85-L96)

## Overview
FunctionCallInfoBaseData is a core PostgreSQL structure that contains the data actually passed to fmgr-called functions, providing the complete calling context including function metadata, arguments, and result information.

## Definition


## Detailed Description
FunctionCallInfoBaseData serves as the primary data structure for PostgreSQL's function manager (fmgr) system, encapsulating all information needed for function calls. This structure is designed to be passed to functions called through the fmgr interface, providing both input parameters and space for output information.

The structure uses a flexible array member for arguments, requiring careful memory allocation through either  for dynamic allocations or  for stack allocations. The structure was renamed from FunctionCallInfoData to FunctionCallInfoBaseData in PostgreSQL v12 to break backward compatibility with code that improperly allocated memory without accounting for argument space.

Called functions are expected to set the  field and potentially modify  or fields within the structure it points to, but should not modify other fields. The argument arrays should be treated as read-only since callers may reuse the same arguments for multiple calls.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing function lookup information, metadata, and handler details
- : Node pointer providing contextual information about the function call environment
- : Node pointer for passing or returning additional information about the function result
- : OID specifying the collation that the function should use for string operations
- : Boolean flag that the called function must set to true if the result is NULL
- : Short integer indicating the number of arguments actually passed to the function
- : Flexible array of NullableDatum structures containing the actual function arguments with their null status

## Dependencies
- Functions called/Symbols referenced:
  - [fmNodePtr](../f/fmNodePtr.md)
  - [NullableDatum](../N/NullableDatum.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [ScalarArrayOpExprHashTable](../S/ScalarArrayOpExprHashTable.md)
  - [FunctionCallInfo](FunctionCallInfo.md) (typedef pointer)
  - SizeForFunctionCallInfo
  - LOCAL_FCINFO
  - pgstat_count_conn_txn_idle_time

## Notes and Other Information
- The structure name includes 'BaseData' rather than just 'Data' to break pre-v12 code that allocated insufficient memory
- Proper memory allocation is critical - use  or  macros
- Functions should treat argument arrays as read-only to support caller reuse
- Field number constants are defined for  (4) and  (6) to support low-level access
- The flexible array member requires C99 or later compiler support
- This is a fundamental structure in PostgreSQL's extensibility architecture, enabling safe and efficient function calls