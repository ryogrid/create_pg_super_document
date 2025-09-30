# makeRelabelType

## Location
[src/backend/nodes/makefuncs.c:451-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L451-L470)

## Overview
Creates a RelabelType node that represents type coercion operations where an expression is relabeled to a different but compatible type.

## Definition
```c
RelabelType *makeRelabelType(Expr *arg, Oid rtype, int32 rtypmod, Oid rcollid, CoercionForm rformat)
```

## Detailed Description
The `makeRelabelType` function creates a RelabelType node that represents a type coercion operation in PostgreSQL. This is used when an expression needs to be treated as a different but binary-compatible type. Unlike full type conversion, relabeling is a lightweight operation that changes the type information without modifying the underlying data representation. The function initializes all the necessary type information and sets the location to -1 (indicating no specific source location).

## Parameters / Member Variables
- `arg`: The expression being relabeled to a different type
- `rtype`: The target type OID that the expression should be treated as
- `rtypmod`: The type modifier for the target type (e.g., precision for numeric types)
- `rcollid`: The collation OID for the target type (relevant for text types)
- `rformat`: The coercion format indicating how the coercion should be displayed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [RelabelType](../R/RelabelType.md) (struct type)
  - CoercionForm (enum type)
- Called from (representative examples):
  - [buildMergedJoinVar](../b/buildMergedJoinVar.md)
  - [coerce_type](../c/coerce_type.md)
  - [assign_hypothetical_collations](../a/assign_hypothetical_collations.md)
  - [make_partition_op_expr](make_partition_op_expr.md)

## Notes and Other Information
- Used primarily for binary-compatible type conversions that require no runtime work
- The location field is set to -1 by default, indicating no specific source location
- Essential for PostgreSQL's type system flexibility, allowing compatible types to be used interchangeably
- Different from full type conversion operations which may require runtime computation
- Commonly used in the parser and type coercion system

## Simplified Source

```c
RelabelType *makeRelabelType(Expr *arg, Oid rtype, int32 rtypmod,
                           Oid rcollid, CoercionForm rformat) {
    RelabelType *node = makeNode(RelabelType);

    // Set the expression being relabeled
    node->arg = arg;

    // Set target type information
    node->resulttype = rtype;
    node->resulttypmod = rtypmod;
    node->resultcollid = rcollid;
    node->relabelformat = rformat;
    node->location = -1;  // No specific source location

    return node;
}
```