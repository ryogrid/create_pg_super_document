# A_Indices

## Location
[src/include/nodes/parsenodes.h:456-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L456-L462)

## Overview
A_Indices represents array subscript or slice bounds in PostgreSQL parse trees, handling both single element access and range slicing operations.

## Definition
```c
typedef struct A_Indices
{
    NodeTag     type;
    bool        is_slice;       /* true if slice (i.e., colon present) */
    Node       *lidx;           /* slice lower bound, if any */
    Node       *uidx;           /* subscript, or slice upper bound if any */
} A_Indices;
```

## Detailed Description
A_Indices is a parse tree node that represents array indexing operations in SQL expressions. It can handle two distinct scenarios: single element access using \[idx\] syntax, and slice operations using \[lidx:uidx\] syntax. For single element access, the uidx field contains the index expression and lidx is NULL. For slice operations, both lidx and uidx can contain expressions, and either can be NULL if omitted (representing open-ended slices). The is_slice flag distinguishes between these two modes of operation.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an A_Indices node
- `is_slice`: Boolean flag indicating whether this represents a slice operation (true) or single subscript access (false)
- `lidx`: Expression node for the lower bound in slice operations, or NULL for single subscripts
- `uidx`: Expression node for the single subscript in non-slice case, or upper bound in slice operations

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - [Node](../N/Node.md) (base node type for expressions)
- Called from (representative examples):
  - [transformIndirection](../t/transformIndirection.md) (src/backend/parser/parse_expr.c:455)
  - [transformContainerSubscripts](../t/transformContainerSubscripts.md) (src/backend/parser/parse_node.c:285)
  - [transformAssignedExpr](../t/transformAssignedExpr.md) (src/backend/parser/parse_target.c:505)
  - [transformAssignmentIndirection](../t/transformAssignmentIndirection.md) (src/backend/parser/parse_target.c:726)
  - [array_subscript_transform](../a/array_subscript_transform.md) (src/backend/utils/adt/arraysubs.c:75)
  - [jsonb_subscript_transform](../j/jsonb_subscript_transform.md) (src/backend/utils/adt/jsonbsubs.c:58)

## Notes and Other Information
- Essential for PostgreSQL's array and container type functionality
- Supports both traditional array indexing and modern slice notation
- Used in transformation of subscript expressions during query parsing
- Integrates with PostgreSQL's subscript infrastructure for various data types including arrays and JSONB
- Part of the indirection mechanism for complex data structure access