# recordDependencyOnSingleRelExpr

## Location
[src/backend/catalog/dependency.c:1596-1697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L1596-L1697)

## Overview
Records dependencies between a database object and all objects referenced within an expression tree, with special handling for dependencies on a single specified relation and its columns.

## Definition

```c
void
recordDependencyOnSingleRelExpr(const ObjectAddress *depender,
								Node *expr, Oid relId,
								DependencyType behavior,
								DependencyType self_behavior,
								bool reverse_self)
```
## Detailed Description
This function is a specialized version of recordDependencyOnExpr designed for expressions that reference only one relation (with varno = 1 and varlevelsup = 0). It provides enhanced control over how dependencies are recorded for the target relation versus other referenced objects, with options for different dependency types and directional control.

The function creates a minimal range table entry for the specified relation and processes the expression tree to find all references. It then separates dependencies into two categories: self-dependencies (references to the specified relation and its columns) and external dependencies (references to other objects), applying different dependency types and potentially reversing the direction for self-dependencies.

Key features:
- Handles single-relation expressions efficiently
- Supports different dependency types for self vs. external references  
- Can reverse dependency direction for column references
- Automatically eliminates duplicate dependencies

## Parameters / Member Variables
- `*depender`: Pointer to ObjectAddress of the object that depends on the expression
- `*expr`: Node tree representing the expression to analyze
- `relId`: OID of the single relation expected to be referenced in the expression
- `behavior`: Dependency type for external object references
- `self_behavior`: Dependency type for references to the specified relation and its columns
- `reverse_self`: If true, column dependencies are reversed (columns depend on table, not vice versa)
## Dependencies
- Functions called/Symbols referenced:
  - [new_object_addresses](../n/new_object_addresses.md)
  - [find_expr_references_walker](../f/find_expr_references_walker.md)
  - [eliminate_duplicate_dependencies](../e/eliminate_duplicate_dependencies.md)
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [recordMultipleDependencies](recordMultipleDependencies.md)
  - [recordDependencyOn](recordDependencyOn.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - list_make1
- Called from (representative examples):
  - [StorePartitionKey](../S/StorePartitionKey.md) (for partition key expressions)
  - [index_create](../i/index_create.md) (for index expressions)
  - [StoreAttrDefault](../S/StoreAttrDefault.md) (for column default expressions)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md) (for constraint expressions)

## Notes and Other Information
- The caller should ensure a whole-table dependency is created separately if needed
- Whole-row Var references (relation.*) do not generate dependency items
- Uses a 'bogus' range table entry with minimal required fields for Var resolution
- The reverse_self option is useful for cases where columns should depend on their containing table
- Memory management handles both regular and self-dependency address collections
- Critical for DDL operations involving single-table expressions like defaults, constraints, and indexes

## Simplified Source

```c
void recordDependencyOnSingleRelExpr(const ObjectAddress *depender,
                                   Node *expr, Oid relId,
                                   DependencyType behavior,
                                   DependencyType self_behavior,
                                   bool reverse_self) {
    find_expr_references_context context;
    RangeTblEntry rte = {0};

    // Create address collection for dependencies
    context.addrs = new_object_addresses();

    // Create minimal range table entry for the relation
    rte.type = T_RangeTblEntry;
    rte.rtekind = RTE_RELATION;
    rte.relid = relId;
    rte.relkind = RELKIND_RELATION;
    rte.rellockmode = AccessShareLock;
    context.rtables = list_make1(list_make1(&rte));

    // Find all object references in the expression
    find_expr_references_walker(expr, &context);
    eliminate_duplicate_dependencies(context.addrs);

    // Separate self-dependencies if special handling needed
    if ((behavior != self_behavior || reverse_self) && context.addrs->numrefs > 0) {
        ObjectAddresses *self_addrs = new_object_addresses();
        ObjectAddress *outobj = context.addrs->refs;
        int outrefs = 0;

        // Separate self-references from external references
        for (int oldref = 0; oldref < context.addrs->numrefs; oldref++) {
            ObjectAddress *thisobj = context.addrs->refs + oldref;

            if (thisobj->classId == RelationRelationId && thisobj->objectId == relId) {
                // Move to self-dependencies
                add_exact_object_address(thisobj, self_addrs);
            } else {
                // Keep as external dependency
                *outobj++ = *thisobj;
                outrefs++;
            }
        }
        context.addrs->numrefs = outrefs;

        // Record self-dependencies with appropriate direction
        if (!reverse_self) {
            recordMultipleDependencies(depender, self_addrs->refs,
                                     self_addrs->numrefs, self_behavior);
        } else {
            // Reverse direction: columns depend on table
            for (int selfref = 0; selfref < self_addrs->numrefs; selfref++) {
                recordDependencyOn(&self_addrs->refs[selfref], depender, self_behavior);
            }
        }

        free_object_addresses(self_addrs);
    }

    // Record external dependencies
    recordMultipleDependencies(depender, context.addrs->refs,
                             context.addrs->numrefs, behavior);

    free_object_addresses(context.addrs);
}
```