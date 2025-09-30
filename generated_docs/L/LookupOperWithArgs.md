# LookupOperWithArgs

## Location
[src/backend/parser/parse_oper.c:133-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L133-L179)

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
  - [LookupTypeNameOid](LookupTypeNameOid.md)
  - [LookupOperName](LookupOperName.md)
  - linitial_node
  - lsecond_node
- Called from (representative examples):
  - [get_object_address](../g/get_object_address.md)
  - [DefineOpClass](../D/DefineOpClass.md)
  - [AlterOpFamilyAdd](../A/AlterOpFamilyAdd.md)
  - [AlterOperator](../A/AlterOperator.md)

## Notes and Other Information
- Requires exactly two arguments in the ObjectWithArgs structure
- NULL arguments are converted to InvalidOid for unary operators
- Used in DDL commands and object address resolution
- Provides type-safe access to operator arguments through structured parsing
- Part of PostgreSQL's object management and DDL processing system

## Simplified Source

```c
Oid
LookupOperWithArgs(ObjectWithArgs *oper, bool noError) {
    TypeName *oprleft, *oprright;
    Oid leftoid, rightoid;

    // Expect exactly two arguments
    Assert(list_length(oper->objargs) == 2);
    oprleft = linitial_node(TypeName, oper->objargs);
    oprright = lsecond_node(TypeName, oper->objargs);

    // Convert type names to OIDs (NULL becomes InvalidOid for unary ops)
    if (oprleft == NULL)
        leftoid = InvalidOid;
    else
        leftoid = LookupTypeNameOid(NULL, oprleft, noError);

    if (oprright == NULL)
        rightoid = InvalidOid;
    else
        rightoid = LookupTypeNameOid(NULL, oprright, noError);

    // Delegate to LookupOperName for actual lookup
    return LookupOperName(NULL, oper->objname, leftoid, rightoid,
                          noError, -1);
}
```