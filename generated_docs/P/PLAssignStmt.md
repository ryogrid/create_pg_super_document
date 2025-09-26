# PLAssignStmt

## Location
src/include/nodes/parsenodes.h: 2224 - 2233

## Overview
PLAssignStmt represents an assignment statement in PL/pgSQL, which is transformed into a SELECT query with UPDATE-like target semantics.

## Definition
```c
typedef struct PLAssignStmt
{
    NodeTag     type;
    
    char       *name;           /* initial column name */
    List       *indirection;    /* subscripts and field names, if any */
    int         nnames;         /* number of names to use in ColumnRef */
    SelectStmt *val;           /* the PL/pgSQL expression to assign */
    ParseLoc    location;       /* name's token location, or -1 if unknown */
} PLAssignStmt;
```

## Detailed Description
PLAssignStmt represents assignment statements within PL/pgSQL function bodies. Unlike regular SQL statements, PL/pgSQL assignments allow setting variables and can include complex indirection for accessing array elements or record fields. The statement is internally transformed into a SELECT query during parsing, but with target list semantics similar to UPDATE statements.

The transformation process handles type coercion using COERCION_PLPGSQL rules (rather than standard COERCION_ASSIGNMENT), allowing for more flexible type conversions appropriate for PL/pgSQL contexts. When indirection is present (array subscripts or field access), the transformation incorporates FieldStore and/or SubscriptingRef nodes to compute new values for container-type variables.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a PLAssignStmt node
- `name`: The initial column/variable name being assigned to
- `indirection`: List of subscripts and field names for array/record access (can be NULL)
- `nnames`: Number of names to use when constructing the ColumnRef for the target
- `val`: SelectStmt containing the PL/pgSQL expression to be assigned
- `location`: Token location of the variable name for error reporting (-1 if unknown)

## Dependencies
- Functions called/Symbols referenced:
  - SelectStmt (for the assignment value expression)
  - ParseLoc (for location tracking)
  - NodeTag (inherited node type system)
  - List (for indirection handling)
- Called from (representative examples):
  - transformStmt (general statement transformation)
  - transformPLAssignStmt (specific PL/pgSQL assignment transformation)
  - raw_expression_tree_walker_impl (for node traversal)

## Notes and Other Information
- Only valid within PL/pgSQL function bodies, not in regular SQL
- Supports complex indirection patterns for arrays and records (e.g., var[1].field := value)
- Uses COERCION_PLPGSQL for more permissive type coercion than standard SQL
- Transformed into CMD_SELECT queries with special target list handling
- Can handle multi-dimensional array assignments and nested record field access
- The nnames field handles cases where the target has multiple dotted components