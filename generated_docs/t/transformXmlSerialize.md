# transformXmlSerialize

## Location
[src/backend/parser/parse_expr.c:2484-2527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2484-L2527)

## Overview
Transforms XMLSERIALIZE expressions during parsing by converting them into XmlExpr nodes with proper type coercion to the target output type.

## Definition

```c
static Node *
transformXmlSerialize(ParseState *pstate, XmlSerialize *xs)
```
## Detailed Description
The  function transforms XMLSERIALIZE expressions during the parsing phase. It creates an XmlExpr node with the IS_XMLSERIALIZE operation, coerces the input expression to XML type, and then applies target type coercion. The function determines the target type from the typename specification and performs implicit casting from TEXT to the target type.

The function allows flexible target types - SQL standard supports CHAR and VARCHAR, but this implementation allows any type that can be implicitly cast from TEXT, enabling user-defined text-like data types to work automatically. If the coercion fails, it reports an appropriate error with location information.

## Parameters / Member Variables
- : ParseState context for the current parsing operation  
- : Input XmlSerialize node to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [coerce_to_specific_type](../c/coerce_to_specific_type.md)
  - [transformExprRecurse](transformExprRecurse.md)
  - [typenameTypeIdAndMod](typenameTypeIdAndMod.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [format_type_be](../f/format_type_be.md)
  - ereport
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- Creates XmlExpr with IS_XMLSERIALIZE operation type
- First coerces input expression to XMLOID, then to target type
- Uses implicit coercion from TEXT to target type for flexibility
- Supports user-defined text-like data types automatically
- Error reporting includes type information and parser position
- The function is located in src/backend/parser/parse_expr.c:2484-2527