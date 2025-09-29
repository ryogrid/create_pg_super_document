## Simplified Source

```c
void assign_expr_collations(ParseState *pstate, Node *expr) {
    assign_collations_context context;

    // Initialize context for tree walk
    context.pstate = pstate;
    context.collation = InvalidOid;
    context.strength = COLLATE_NONE;
    context.location = -1;

    // Walk the expression tree and assign collations
    assign_collations_walker(expr, &context);
}
```