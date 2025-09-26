# SubscriptingRef

## Location
src/include/nodes/primnodes.h: 679 - 704

## Overview
SubscriptingRef describes a subscripting operation over a container (such as arrays), supporting both fetching and storing operations for single elements or slices of the container.

## Definition


## Detailed Description
SubscriptingRef is a comprehensive expression node that handles all forms of subscripting operations on container types in PostgreSQL, primarily arrays but also other subscriptable types like JSONB. It supports four main operations: fetching single elements, fetching slices, storing single elements, and storing slices.

The structure can represent both simple subscripting (e.g., ) and slice operations (e.g., ). For slice operations, both lower and upper bounds are specified through separate expression lists. When  is NIL, the operation targets a single element; otherwise, it targets a slice.

The implementation allows for in-place modifications when dealing with read-write expanded containers, providing performance optimizations for large container operations.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : OID of the actual container type that determines subscripting semantics
- : OID of the container's element type (saved for subscripting functions)
- : OID of the SubscriptingRef operation's result type
- : Type modifier of the result
- : Collation OID of the result, or InvalidOid if none
- : List of expressions evaluating to upper container indexes
- : List of expressions evaluating to lower container indexes (NIL for single element operations)
- : Expression that evaluates to the container value being subscripted
- : Expression for the source value in assignment operations (NULL for fetch operations)

## Dependencies
- Functions called/Symbols referenced:
  - Expr (base expression structure and container expressions)
  - List (for index expressions)
  - Oid (for type references)
  
- Called from (representative examples):
  - ExecInitSubscriptingRef (executor initialization for subscripting operations)
  - transformContainerSubscripts (parser transformation of subscript expressions)
  - array_subscript_transform, jsonb_subscript_transform (type-specific subscripting transformations)
  - processIndirection (rule output processing for subscript operations)
  - transformAssignmentSubscripts (parser handling of assignment to subscripted containers)

## Notes and Other Information
- Supports both fetch and store operations - the presence of  determines the operation type
- Individual expressions in slice subscript lists can be NULL, meaning "use the container's current bound"
- Type information fields are marked as query_jumble_ignore for consistent plan caching
- For slice operations,  and  must have the same length when both are present
- The result type can vary: element type for single element fetch, container type for slice operations or stores
- Extensible design allows different container types to implement custom subscripting semantics
- Supports PostgreSQL's advanced array operations including multi-dimensional arrays and complex slice operations
- Performance-optimized for expanded container objects, allowing in-place modifications when possible