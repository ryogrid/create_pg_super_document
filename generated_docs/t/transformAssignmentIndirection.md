# transformAssignmentIndirection

## Location
[src/backend/parser/parse_target.c:683-902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L683-L902)

## Overview
Processes indirection (field selection or subscripting) of target columns in INSERT/UPDATE/assignment statements, recursively handling multiple levels of complex assignments to composite types and arrays.

## Definition

```c
Node *
transformAssignmentIndirection(ParseState *pstate,
							   Node *basenode,
							   const char *targetName,
							   bool targetIsSubscripting,
							   Oid targetTypeId,
							   int32 targetTypMod,
							   Oid targetCollation,
							   List *indirection,
							   ListCell *indirection_cell,
							   Node *rhs,
							   CoercionContext ccontext,
							   int location)
```
## Detailed Description
This function is the core processor for complex assignment operations involving field selection and array subscripting in PostgreSQL. It handles assignments like  or  by recursively processing the indirection chain.

Key behaviors:
1. **Substitution Setup**: Uses CaseTestExpr nodes as placeholders when basenode is NULL to enable recursive processing
2. **Indirection Parsing**: Separates field selections from array subscripts, treating adjacent A_Indices nodes as multidimensional subscript operations
3. **Field Processing**: For String nodes (field names), validates composite types, looks up field attributes, and builds FieldStore nodes
4. **Subscript Delegation**: Calls transformAssignmentSubscripts for array subscript operations
5. **Type Coercion**: Applies final type coercion and domain constraints as needed
6. **Error Handling**: Provides detailed error messages for unsupported operations and type mismatches

The function distinguishes between different base node contexts:
- UPDATE: basenode is a Var for the target column
- INSERT: basenode is a null Const of the target type
- PL/pgSQL: basenode is a Param for the target variable

## Parameters / Member Variables
- : Parse state containing context for the current query parsing
- : Base node representing the target (Var for UPDATE, null Const for INSERT, Param for PL/pgSQL)
- : Name of the field or subfield being assigned to (for error reporting)
- : Boolean indicating if the operation involves subscripting (affects error messages)
- : Data type OID of the object being assigned to
- : Type modifier of the target object
- : Collation of the target object
- : List of indirection nodes (field names, subscripts)
- : Current position in the indirection list for recursive processing
- : Right-hand side expression to be assigned (already transformed)
- : Coercion context level (COERCION_ASSIGNMENT for normal statements)
- : Cursor position for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (CaseTestExpr)
  - for_each_cell
  - [lappend](../l/lappend.md)
  - IsA (A_Indices, A_Star, String)
  - [transformAssignmentSubscripts](transformAssignmentSubscripts.md)
  - [getBaseTypeAndTypmod](../g/getBaseTypeAndTypmod.md)
  - [typeidTypeRelid](typeidTypeRelid.md)
  - [get_attnum](../g/get_attnum.md)
  - [get_atttypetypmodcoll](../g/get_atttypetypmodcoll.md)
  - strVal
  - list_make1
  - list_make1_int
  - [lnext](../l/lnext.md)
  - [coerce_to_domain](../c/coerce_to_domain.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [exprType](../e/exprType.md)
  - [format_type_be](../f/format_type_be.md)
  - Constants: InvalidAttrNumber, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST
- Called from:
  - [transformPLAssignStmt](transformPLAssignStmt.md) (analyze.c)
  - [transformAssignedExpr](transformAssignedExpr.md) (parse_target.c)
  - [transformAssignmentIndirection](transformAssignmentIndirection.md) (recursive)
  - [transformAssignmentSubscripts](transformAssignmentSubscripts.md) (parse_target.c)

## Notes and Other Information
- The function is recursive and can handle arbitrarily deep indirection chains
- CaseTestExpr is used as a safe placeholder because only FieldStore and SubscriptingRef nodes will be above it in the expression tree
- Row expansion via '*' is explicitly not supported and triggers an error
- System columns cannot be assigned to in composite types
- Domain constraints are applied after field assignment, though this may lead to unwanted failures for partial updates (the rewriter merges subfield assignments to mitigate this)
- Adjacent A_Indices nodes are treated as a single multidimensional subscript operation
- The function provides different error messages for subscripting vs field assignment failures
- Critical for implementing PostgreSQL's complex assignment semantics for composite types and arrays

## Simplified Source

```c
Node *
transformAssignmentIndirection(ParseState *pstate,
                               Node *basenode,
                               const char *targetName,
                               bool targetIsSubscripting,
                               Oid targetTypeId,
                               int32 targetTypMod,
                               Oid targetCollation,
                               List *indirection,
                               ListCell *indirection_cell,
                               Node *rhs,
                               CoercionContext ccontext,
                               int location)
{
    Node *result;
    List *subscripts = NIL;
    ListCell *i;

    if (indirection_cell && !basenode) {
        // Set up a substitution using CaseTestExpr as placeholder
        CaseTestExpr *ctest = makeNode(CaseTestExpr);
        ctest->typeId = targetTypeId;
        ctest->typeMod = targetTypMod;
        ctest->collation = targetCollation;
        basenode = (Node *) ctest;
    }

    // Split field-selection operations from subscripting
    for_each_cell(i, indirection, indirection_cell) {
        Node *n = lfirst(i);

        if (IsA(n, A_Indices))
            subscripts = lappend(subscripts, n);
        else if (IsA(n, A_Star)) {
            ereport(ERROR, /* row expansion not supported */);
        }
        else {
            // Field selection
            Assert(IsA(n, String));

            // Process any pending subscripts first
            if (subscripts) {
                return transformAssignmentSubscripts(pstate,
                                                     basenode, targetName,
                                                     targetTypeId, targetTypMod, targetCollation,
                                                     subscripts, indirection, i,
                                                     rhs, ccontext, location);
            }

            // Look up the composite type
            int32 baseTypeMod = targetTypMod;
            Oid baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypeMod);
            Oid typrelid = typeidTypeRelid(baseTypeId);

            if (!typrelid)
                ereport(ERROR, /* not a composite type */);

            // Look up field
            AttrNumber attnum = get_attnum(typrelid, strVal(n));
            if (attnum == InvalidAttrNumber)
                ereport(ERROR, /* field does not exist */);
            if (attnum < 0)
                ereport(ERROR, /* cannot assign to system column */);

            Oid fieldTypeId;
            int32 fieldTypMod;
            Oid fieldCollation;
            get_atttypetypmodcoll(typrelid, attnum,
                                  &fieldTypeId, &fieldTypMod, &fieldCollation);

            // Recurse for nested indirection
            rhs = transformAssignmentIndirection(pstate, NULL, strVal(n), false,
                                                 fieldTypeId, fieldTypMod, fieldCollation,
                                                 indirection, lnext(indirection, i),
                                                 rhs, ccontext, location);

            // Build FieldStore node
            FieldStore *fstore = makeNode(FieldStore);
            fstore->arg = (Expr *) basenode;
            fstore->newvals = list_make1(rhs);
            fstore->fieldnums = list_make1_int(attnum);
            fstore->resulttype = baseTypeId;

            // Apply domain constraints if needed
            if (baseTypeId != targetTypeId)
                return coerce_to_domain((Node *) fstore, baseTypeId, baseTypeMod,
                                        targetTypeId, COERCION_IMPLICIT,
                                        COERCE_IMPLICIT_CAST, location, false);

            return (Node *) fstore;
        }
    }

    // Process trailing subscripts if any
    if (subscripts) {
        return transformAssignmentSubscripts(pstate, basenode, targetName,
                                             targetTypeId, targetTypMod, targetCollation,
                                             subscripts, indirection, NULL,
                                             rhs, ccontext, location);
    }

    // Base case: coerce RHS to match target type
    result = coerce_to_target_type(pstate, rhs, exprType(rhs),
                                   targetTypeId, targetTypMod,
                                   ccontext, COERCE_IMPLICIT_CAST, -1);
    if (result == NULL) {
        if (targetIsSubscripting)
            ereport(ERROR, /* subscripted assignment type mismatch */);
        else
            ereport(ERROR, /* subfield assignment type mismatch */);
    }

    return result;
}
```