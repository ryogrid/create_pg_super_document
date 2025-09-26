# CreateConversionCommand

## Location
[src/backend/commands/conversioncmds.c:32-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/conversioncmds.c#L32-L134)

## Overview
Implements the CREATE CONVERSION SQL command by validating conversion parameters and creating a new encoding conversion in the PostgreSQL system catalog.

## Definition

```c
ObjectAddress
CreateConversionCommand(CreateConversionStmt *stmt)
```
## Detailed Description
CreateConversionCommand processes a CREATE CONVERSION statement to create a new encoding conversion function in PostgreSQL. The function performs comprehensive validation including namespace permissions, encoding name validity, conversion function signature verification, and functional testing of the conversion before registering it in the system catalog.

The function enforces several important constraints:
- Conversions to or from SQL_ASCII are explicitly prohibited as they are considered meaningless
- The conversion function must have a specific signature with 6 parameters and return int4
- The conversion function is tested with empty input to verify compatibility with the specified encodings
- Users must have CREATE privileges on the target namespace and EXECUTE privileges on the conversion function

## Parameters / Member Variables
- : Pointer to CreateConversionStmt containing the parsed CREATE CONVERSION statement with conversion name, source/destination encodings, conversion function name, and default flag

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [pg_char_to_encoding](../p/pg_char_to_encoding.md)
  - [LookupFuncName](../L/LookupFuncName.md)
  - [get_func_rettype](../g/get_func_rettype.md)
  - [NameListToString](../N/NameListToString.md)
  - OidFunctionCall6
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [ConversionCreate](ConversionCreate.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
The function performs a unique validation step by actually calling the conversion function with empty string input to ensure it works correctly for the specified encoding pair. This prevents registration of incompatible conversion functions. The function signature validation enforces the standard PostgreSQL conversion function interface: (int4, int4, cstring, internal, int4, bool) returning int4. Located at src/backend/commands/conversioncmds.c:32-134.