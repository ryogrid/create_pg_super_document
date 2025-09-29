# op_signature_string

## Location
[src/backend/parser/parse_oper.c:602-621](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L602-L621)

## Overview
The  function builds a string representation of an operator name including its argument types, primarily used for error message construction.

## Definition

```c
const char *
op_signature_string(List *op, Oid arg1, Oid arg2)
```
## Detailed Description
This utility function constructs a human-readable string representation of an operator signature that includes both the operator name and its argument types. The resulting string follows the format "type1 operator type2" for binary operators or "operator type2" for unary operators. This function is primarily used in error reporting to provide clear and informative messages when operator lookups fail.

The function handles both unary and binary operators by checking the validity of the first argument OID. For unary operators, only the second argument type is included in the signature string.

## Parameters / Member Variables
- : List containing the operator name components
- : OID of the first argument type (InvalidOid for unary operators)
- : OID of the second argument type (or the only argument for unary operators)

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [format_type_be](../f/format_type_be.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [NameListToString](../N/NameListToString.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
- Called from (representative examples):
  - [ValidateOperatorReference](../V/ValidateOperatorReference.md)
  - [LookupOperName](../L/LookupOperName.md)
  - [compatible_oper](../c/compatible_oper.md)
  - [op_error](op_error.md)
  - [make_op](../m/make_op.md)
  - [make_scalar_array_op](../m/make_scalar_array_op.md)

## Notes and Other Information
- Returns a palloc'd string buffer that should be freed by the caller
- The function always includes the second argument type, making it suitable for both unary and binary operators
- Commonly used in error message construction throughout the PostgreSQL parser
- The resulting string format makes operator signatures easily readable for users and developers

## Simplified Source

```c
const char *op_signature_string(List *op, Oid arg1, Oid arg2) {
    StringInfoData argbuf;

    // Initialize string buffer
    initStringInfo(&argbuf);

    // Add first argument type if valid (for binary operators)
    if (OidIsValid(arg1)) {
        appendStringInfo(&argbuf, "%s ", format_type_be(arg1));
    }

    // Add operator name
    appendStringInfoString(&argbuf, NameListToString(op));

    // Add second argument type
    appendStringInfo(&argbuf, " %s", format_type_be(arg2));

    return argbuf.data; // Return formatted string
}
```