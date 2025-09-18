# GetAttributeCompression

## Location
[src/backend/commands/tablecmds.c:20230-20267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L20230-L20267)

## Overview
Resolves a column compression specification string to a compression method identifier, validating that the data type supports compression.

## Definition
```c
static char GetAttributeCompression(Oid atttypid, const char *compression)
```

## Detailed Description
This function converts a string-based compression specification into a compression method character code. It performs validation to ensure that the specified compression method is valid and that the column's data type supports compression (is toastable). If no compression is specified or "default" is specified, it returns InvalidCompressionMethod. The function enforces that only toastable data types can have non-default compression methods, providing user-friendly error messages for invalid configurations.

## Parameters / Member Variables
- `atttypid`: The OID of the attribute's data type
- `compression`: String specifying the compression method (can be NULL, "default", or a specific compression method name)

## Dependencies
- Functions called/Symbols referenced:
  - InvalidCompressionMethod
  - TypeIsToastable
  - [CompressionNameToMethod](../C/CompressionNameToMethod.md)
  - CompressionMethodIsValid
- Called from (representative examples):
  - [BuildDescForRelation](../B/BuildDescForRelation.md)
  - [ATExecSetCompression](../A/ATExecSetCompression.md)

## Notes and Other Information
- Returns InvalidCompressionMethod for NULL or "default" compression specifications
- Validates that the data type is toastable before allowing non-default compression methods
- Provides clear error messages for unsupported data types and invalid compression methods
- The function intentionally allows attcompression and attstorage to be independent settings
- Used during table creation and ALTER TABLE operations to set column compression