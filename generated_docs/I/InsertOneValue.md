# InsertOneValue

## Location
src/backend/bootstrap/bootstrap.c: 626 - 663

## Overview
InsertOneValue converts a string value to its internal PostgreSQL representation and stores it in the global values array for the specified column position during bootstrap.

## Definition
```c
void InsertOneValue(char *value, int i)
```

## Detailed Description
InsertOneValue is a bootstrap function that processes individual column values during PostgreSQL system initialization. The function takes a string representation of a value and converts it to the appropriate internal format for storage in a specific column position.

The conversion process involves several steps:
1. Validates the column index is within acceptable bounds (0 to MAXATTR-1)
2. Retrieves the data type OID for the specified column from the relation descriptor
3. Obtains type-specific I/O information including input/output functions and parameters
4. Uses the type-specific input function to convert the string value to internal representation
5. Stores the converted value in the global values array at the specified index
6. Optionally logs the converted value using the types output function for debugging

This function works in conjunction with InsertOneNull and InsertOneTuple to build complete rows during bootstrap initialization.

## Parameters / Member Variables
- `value`: String representation of the value to be inserted
- `i`: Zero-based column index where the value should be stored (must be 0 <= i < MAXATTR)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (accesses attribute information from tuple descriptor)
  - boot_get_type_io_data (retrieves type I/O function information)
  - OidInputFunctionCall (calls type-specific input function for conversion)
  - OidOutputFunctionCall (calls type-specific output function for debug logging)
  - ereport (structured error/debug reporting)
  - errmsg_internal (internal error message formatting)
  - MAXATTR (maximum number of attributes constant)
  - DEBUG4 (debug logging level)
- Called from (representative examples):
  - Bootstrap parser when processing column values

## Notes and Other Information
- This function is part of the bootstrap process and only used during PostgreSQL system initialization
- The function performs bounds checking with Assert to ensure column index validity
- Uses ereport instead of elog for debug output to avoid parameter evaluation when logging is disabled
- The converted value is stored in the global `values` array which is later used by InsertOneTuple
- Type conversion uses the PostgreSQL type systems input functions, ensuring proper data validation and formatting
- Debug logging shows both the original string value and the converted internal representation
- Operates on the assumption that a relation is currently open (boot_reldesc is valid)
- The function handles all PostgreSQL data types through their registered input functions