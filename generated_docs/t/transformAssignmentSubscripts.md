# transformAssignmentSubscripts

## Location
[src/backend/parser/parse_target.c:903-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L903-L1014)

## Overview
Helper function for transformAssignmentIndirection that specifically processes container assignment operations involving array subscripts and container element assignments.

## Definition

```c
static Node *
transformAssignmentSubscripts(ParseState *pstate,
							  Node *basenode,
							  const char *targetName,
							  Oid targetTypeId,
							  int32 targetTypMod,
							  Oid targetCollation,
							  List *subscripts,
							  List *indirection,
							  ListCell *next_indirection,
							  Node *rhs,
							  CoercionContext ccontext,
							  int location)
```
## Detailed Description
This static function is a specialized helper that handles the complex process of transforming subscript-based assignments to containers (primarily arrays) in PostgreSQL. It works in coordination with transformAssignmentIndirection to process assignments like  or .

The function performs several key operations:
1. **Container Type Identification**: Uses transformContainerType to identify the actual container type, handling domains over container types
2. **Subscript Processing**: Calls transformContainerSubscripts to process the subscript expressions and build a SubscriptingRef node
3. **Type Determination**: Determines the required type for the RHS based on the container's element type
4. **Collation Handling**: Manages collation inheritance from container to elements, with special handling for domains
5. **Recursive Processing**: Recursively calls transformAssignmentIndirection for any remaining indirection
6. **Result Assembly**: Constructs the final SubscriptingRef node with proper type information
7. **Domain Coercion**: Applies coercion to domains over container types when necessary

The function handles the distinction between direct container assignments and assignments through domains, ensuring proper type coercion and constraint application.

## Parameters / Member Variables
- `*pstate`: Parse state containing context for the current query parsing
- `*basenode`: Base node representing the container being subscripted
- `*targetName`: Name of the target being assigned to (for error reporting)
- `targetTypeId`: Data type OID of the target container
- `targetTypMod`: Type modifier of the target container
- `targetCollation`: Collation of the target container
- `*subscripts`: List of subscript expressions (A_Indices nodes)
- `*indirection`: Complete list of indirection nodes for recursive processing
- `*next_indirection`: Position in indirection list for further processing
- `*rhs`: Right-hand side expression to be assigned
- `ccontext`: Coercion context level for type conversions
- `location`: Cursor position for error reporting
## Dependencies
- Functions called/Symbols referenced:
  - [transformContainerType](transformContainerType.md)
  - [transformContainerSubscripts](transformContainerSubscripts.md)
  - [get_typcollation](../g/get_typcollation.md)
  - [transformAssignmentIndirection](transformAssignmentIndirection.md) (recursive call)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [exprType](../e/exprType.md)
  - [format_type_be](../f/format_type_be.md)
  - Constants: COERCE_IMPLICIT_CAST
- Called from:
  - [transformAssignmentIndirection](transformAssignmentIndirection.md) (twice - for embedded and trailing subscripts)

## Notes and Other Information
- This is a static function, only accessible within parse_target.c
- The function is specifically designed to handle multidimensional array assignments
- Collation handling includes special logic for domains over container types
- The function can handle mixed indirection patterns (subscripts followed by field access)
- Type coercion failures for int2vector/oidvector are handled differently than true domain failures
- The SubscriptingRef node is modified in place to set the assignment expression and restore container type information
- Critical for implementing PostgreSQL's array and container assignment semantics
- Works closely with the rewriter to handle multiple assignments to the same container in a single statement
- The function assumes that subscripts list is not empty (NIL) as enforced by assertion

## Simplified Source

```c
static Node *transformAssignmentSubscripts(ParseState *pstate,
                                         Node *basenode,
                                         const char *targetName,
                                         Oid targetTypeId,
                                         int32 targetTypMod,
                                         Oid targetCollation,
                                         List *subscripts,
                                         List *indirection,
                                         ListCell *next_indirection,
                                         Node *rhs,
                                         CoercionContext ccontext,
                                         int location) {
    SubscriptingRef *sbsref;
    Oid containerType = targetTypeId;
    int32 containerTypMod = targetTypMod;

    // Identify actual container type (handle domains)
    transformContainerType(&containerType, &containerTypMod);

    // Process subscripts and create SubscriptingRef node
    sbsref = transformContainerSubscripts(pstate, basenode, containerType,
                                        containerTypMod, subscripts, true);

    // Determine required type for RHS
    Oid typeNeeded = sbsref->refrestype;
    int32 typmodNeeded = sbsref->reftypmod;

    // Handle collation (special case for domains over containers)
    Oid collationNeeded = (containerType == targetTypeId) ?
                         targetCollation : get_typcollation(containerType);

    // Recursively process remaining indirection
    rhs = transformAssignmentIndirection(pstate, NULL, targetName, true,
                                       typeNeeded, typmodNeeded, collationNeeded,
                                       indirection, next_indirection, rhs,
                                       ccontext, location);

    // Set assignment expression and restore container type
    sbsref->refassgnexpr = (Expr *) rhs;
    sbsref->refrestype = containerType;
    sbsref->reftypmod = containerTypMod;

    Node *result = (Node *) sbsref;

    // Coerce to domain if target was domain over container
    if (containerType != targetTypeId) {
        result = coerce_to_target_type(pstate, result, exprType(result),
                                     targetTypeId, targetTypMod, ccontext,
                                     COERCE_IMPLICIT_CAST, -1);
        if (result == NULL)
            ereport(ERROR, "cannot cast type to target type");
    }

    return result;
}
```