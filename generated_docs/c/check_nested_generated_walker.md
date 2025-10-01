# check_nested_generated_walker

## Location
[src/backend/catalog/heap.c:2746-2787](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L2746-L2787)

## Overview
A static walker function that recursively traverses expression nodes to detect and prevent references to generated columns within column generation expressions, ensuring that generated columns do not depend on other generated columns.

## Definition
static bool check_nested_generated_walker(Node *node, void *context)

## Detailed Description
This function implements a node tree walker that validates expressions used in generated column definitions. It specifically prevents two types of invalid references:
1. Direct references to other generated columns (which would create dependency chains)
2. Whole-row variable references (which would cause self-referential dependencies)

The function operates as part of PostgreSQL's expression validation system, using the standard tree walker pattern to recursively examine all nodes in an expression tree. When it encounters a Var node (column reference), it checks whether the referenced column is generated and raises appropriate errors if violations are found.

## Parameters / Member Variables
- `node`: The current Node being examined in the expression tree traversal
- `context`: A ParseState pointer containing parser state information including the range table

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch: Retrieves relation information from the range table
  - [get_attgenerated](../g/get_attgenerated.md): Checks if an attribute is a generated column
  - [get_attname](../g/get_attname.md): Gets the name of an attribute for error reporting
  - expression_tree_walker: Recursively walks the expression tree
  - ereport: Reports errors with detailed messages

- Called from (representative examples):
  - [check_nested_generated](check_nested_generated.md): Main entry point for generated column validation
  - [check_nested_generated_walker](check_nested_generated_walker.md): Recursive self-calls during tree traversal

## Notes and Other Information
- This is a static function used internally within heap.c for generated column validation
- The function follows PostgreSQL's standard tree walker pattern, returning false to continue traversal or true to stop
- Error messages provide specific details about why the reference is invalid, including column names and parser positions
- System columns are explicitly excluded from validation as they are handled separately in the parser
- The function is part of PostgreSQL's generated column feature introduced to maintain data consistency

## Simplified Source

```c
static bool check_nested_generated_walker(Node *node, void *context) {
    ParseState *pstate = context;

    if (node == NULL)
        return false;

    if (IsA(node, Var)) {
        Var *var = (Var *) node;

        // Get relation and attribute info
        Oid relid = rt_fetch(var->varno, pstate->p_rtable)->relid;
        if (!OidIsValid(relid))
            return false;

        AttrNumber attnum = var->varattno;

        // Check for reference to another generated column
        if (attnum > 0 && get_attgenerated(relid, attnum)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cannot use generated column \"%s\" in column generation expression",
                            get_attname(relid, attnum, false)),
                     errdetail("A generated column cannot reference another generated column."),
                     parser_errposition(pstate, var->location)));
        }

        // Prohibit whole-row variable references (self-referential)
        if (attnum == 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("cannot use whole-row variable in column generation expression"),
                     errdetail("This would cause the generated column to depend on its own value."),
                     parser_errposition(pstate, var->location)));
        }

        return false;
    }

    // Continue walking the expression tree
    return expression_tree_walker(node, check_nested_generated_walker, context);
}
```