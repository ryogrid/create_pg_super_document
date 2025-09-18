# LookupOperWithArgs

## Location
src/backend/parser/parse_oper.c: 133 - 179

## Overview
Looks up an operator by name where the argument types are specified through an ObjectWithArgs node, providing a higher-level interface than LookupOperName.

## Definition
```c
Oid LookupOperWithArgs(ObjectWithArgs *oper, bool noError)
```

## Detailed Description
LookupOperWithArgs is a wrapper around LookupOperName that extracts operator information from an ObjectWithArgs structure. It handles the conversion of TypeName arguments to OIDs and supports both unary (prefix) and binary operators. The function expects exactly two arguments in the ObjectWithArgs structure, where NULL values indicate missing operands for unary operators. It delegates the actual operator lookup to LookupOperName after resolving the type names.

## Parameters / Member Variables
- `oper`: ObjectWithArgs structure containing operator name and argument type specifications
- `noError`: If true, return InvalidOid on failure; if false, raise an error

## Dependencies
- Functions called/Symbols referenced:
  - LookupTypeNameOid
  - LookupOperName
  - linitial_node
  - lsecond_node
- Called from (representative examples):
  - get_object_address
  - DefineOpClass
  - AlterOpFamilyAdd
  - AlterOperator

## Notes and Other Information
- Requires exactly two arguments in the ObjectWithArgs structure
- NULL arguments are converted to InvalidOid for unary operators
- Used in DDL commands and object address resolution
- Provides type-safe access to operator arguments through structured parsing
- Part of PostgreSQL's object management and DDL processing system