# PLyUnicode_ToComposite

## Location
[src/pl/plpython/plpy_typeio.c:1281-1341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1281-L1341)

## Overview
Converts a Python string to a PostgreSQL composite type by parsing it as a record literal using the record_in function.

## Definition


## Detailed Description
This function handles the conversion of Python string objects into PostgreSQL composite types by interpreting the string as a record literal. It sets up and calls the built-in record_in function to perform the parsing. The function includes special error handling for array contexts to provide helpful hints when users inadvertently create malformed record literals due to changes in multi-dimensional array interpretation introduced in PostgreSQL 10.

The conversion process involves:
1. Lazy initialization of the record_in function call info if not already set up
2. Conversion of Python string to C string representation
3. Special validation for array contexts to detect common user errors
4. Delegation to PostgreSQL's built-in record_in function for actual parsing

## Parameters / Member Variables
- : PLyObToDatum structure containing composite type information and function call context
- : Python string object containing the record literal to be parsed
- : Boolean flag indicating whether this conversion is happening within an array context

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (initializes function call info for record_in)
  - [PLyObject_AsString](PLyObject_AsString.md) (converts Python string to C string)
  - [InputFunctionCall](../I/InputFunctionCall.md) (calls PostgreSQL's record_in function)
  - OidIsValid (checks if function OID is valid)
- Called from (representative examples):
  - [PLyObject_ToComposite](PLyObject_ToComposite.md) (src/pl/plpython/plpy_typeio.c:959)

## Notes and Other Information
- Uses lazy initialization of record_in function call info to avoid repeated setup overhead
- Includes special error detection and user-friendly hints for PostgreSQL 10+ compatibility issues
- The inarray parameter triggers enhanced error reporting for common migration issues from pre-PostgreSQL 10 versions
- When inarray is true, validates that the string starts with '(' to catch malformed record literals
- Provides specific error hints suggesting the use of Python tuples instead of nested lists for composite array elements
- The error checking logic is designed to match record_in's validation to avoid false positives