# NextCopyFrom

## Location
[src/backend/commands/copyfromparse.c:854-1098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L854-L1098)

## Overview
NextCopyFrom reads and processes the next complete tuple from a COPY FROM operation, handling both text/CSV and binary formats while applying type conversions, defaults, and error handling.

## Definition

```c
bool
NextCopyFrom(CopyFromState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
```
## Detailed Description
This function is the main entry point for reading and processing individual tuples during COPY FROM operations. It handles both text/CSV and binary input formats, performing complete tuple construction including type conversion, default value evaluation, and error handling. For text/CSV mode, it calls NextCopyFromRawFields to get raw field data, then processes each field through input functions and applies FORCE_NULL/FORCE_NOT_NULL options. For binary mode, it reads the field count and binary data directly. The function also handles default expressions for columns not present in the input data and implements error recovery when ON_ERROR IGNORE is specified.

The function initializes all output arrays to NULL/true, then populates them based on the input data format. It ensures proper type conversion using the relation's input functions and handles various COPY options like null handling and default expressions. Error handling includes soft error recovery and detailed logging when configured.

## Parameters / Member Variables
- : The COPY FROM state structure containing configuration, input functions, and parsing state
- : Expression context for evaluating default expressions; can be NULL if no defaults are used
- : Output array of Datum values, one per relation column, filled by this function
- : Output array of null indicators, one per relation column, filled by this function

## Dependencies
- Functions called/Symbols referenced:
  - [NextCopyFromRawFields](NextCopyFromRawFields.md): Reads raw field strings from input for text/CSV mode
  - MemSet: Initializes arrays to default values
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md): Safely converts string input to typed Datum values
  - [ExecEvalExpr](../E/ExecEvalExpr.md): Evaluates default expressions for missing columns
  - [CopyGetInt16](../C/CopyGetInt16.md): Reads 16-bit integers from binary input
  - [CopyReadBinaryData](../C/CopyReadBinaryData.md): Reads raw binary data from input stream
  - [CopyReadBinaryAttribute](../C/CopyReadBinaryAttribute.md): Reads and converts binary attribute data
  - CopyLimitPrintoutLength: Limits output length for error messages
  - lfirst_int: Extracts integer values from list cells
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md): Main COPY FROM processing loop

## Notes and Other Information
- Supports both text/CSV and binary input formats with format-specific processing paths
- Handles FORCE_NULL and FORCE_NOT_NULL options in CSV mode for flexible null handling
- Implements soft error recovery when ON_ERROR IGNORE is specified, allowing processing to continue
- Requires proper memory context setup when default expressions are used (per-tuple context)
- Returns false when no more tuples are available (EOF or end of data)
- Maintains current line number and attribute name for error reporting context
- All output arrays must be pre-allocated to match the relation's column count
- Default expressions are evaluated after input processing for columns not in the input data