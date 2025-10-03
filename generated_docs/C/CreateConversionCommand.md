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
- `*stmt`: Pointer to CreateConversionStmt containing the parsed CREATE CONVERSION statement with conversion name, source/destination encodings, conversion function name, and default flag
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

## Simplified Source

```c
ObjectAddress
CreateConversionCommand(CreateConversionStmt *stmt)
{
    Oid namespaceId;
    char *conversion_name;
    int from_encoding, to_encoding;
    Oid funcoid;
    static const Oid funcargs[] = {INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID};

    // Get namespace and check creation privileges
    namespaceId = QualifiedNameGetCreationNamespace(stmt->conversion_name, &conversion_name);

    AclResult aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));
    }

    // Validate encoding names exist
    from_encoding = pg_char_to_encoding(stmt->for_encoding_name);
    if (from_encoding < 0) {
        ereport(ERROR, "source encoding does not exist");
    }

    to_encoding = pg_char_to_encoding(stmt->to_encoding_name);
    if (to_encoding < 0) {
        ereport(ERROR, "destination encoding does not exist");
    }

    // Reject SQL_ASCII conversions (not supported)
    if (from_encoding == PG_SQL_ASCII || to_encoding == PG_SQL_ASCII) {
        ereport(ERROR, "conversion to/from SQL_ASCII not supported");
    }

    // Find and validate conversion function
    funcoid = LookupFuncName(stmt->func_name, sizeof(funcargs) / sizeof(Oid), funcargs, false);

    // Check function returns int4
    if (get_func_rettype(funcoid) != INT4OID) {
        ereport(ERROR, "conversion function must return integer");
    }

    // Check execute permission on function
    aclresult = object_aclcheck(ProcedureRelationId, funcoid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK) {
        aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString(stmt->func_name));
    }

    // Test conversion function with empty input
    char result[1];
    Datum funcresult = OidFunctionCall6(funcoid,
                                      Int32GetDatum(from_encoding),
                                      Int32GetDatum(to_encoding),
                                      CStringGetDatum(""),
                                      CStringGetDatum(result),
                                      Int32GetDatum(0),
                                      BoolGetDatum(false));

    // Function should return 0 for empty input
    if (DatumGetInt32(funcresult) != 0) {
        ereport(ERROR, "conversion function returned incorrect result for empty input");
    }

    // Create the conversion in system catalog
    return ConversionCreate(conversion_name, namespaceId, GetUserId(),
                           from_encoding, to_encoding, funcoid, stmt->def);
}
```