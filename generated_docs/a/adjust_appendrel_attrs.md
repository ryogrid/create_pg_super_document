# adjust_appendrel_attrs

## Location
[src/backend/optimizer/util/appendinfo.c:196-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/appendinfo.c#L196-L214)

## Overview
Transforms query tree nodes by translating variable references from parent relations to their corresponding child relations using provided AppendRelInfo mappings.

## Definition

```c
Node *
adjust_appendrel_attrs(PlannerInfo *root, Node *node, int nappinfos,
					   AppendRelInfo **appinfos)
```
## Detailed Description
This function serves as the main entry point for translating parent relation references to child relation references in query trees. It sets up the necessary context and delegates the actual transformation work to adjust_appendrel_attrs_mutator. The function is essential for inheritance and partitioning scenarios where queries against parent tables need to be rewritten to operate on specific child tables. It handles not just Var nodes but also other elements like range table indexes that appear outside of variable references.

## Parameters / Member Variables
- : PlannerInfo containing planning context and information
- : The query tree node to be transformed (can be any Node type)
- : Number of AppendRelInfo structures in the array
- : Array of AppendRelInfo structures containing parent-to-child translation mappings

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_appendrel_attrs_mutator](adjust_appendrel_attrs_mutator.md) (performs the actual tree transformation)
  - adjust_appendrel_attrs_context (context structure for transformation)
- Called from (representative examples):
  - [set_append_rel_size](../s/set_append_rel_size.md)
  - [add_child_rel_equivalences](add_child_rel_equivalences.md)
  - [try_partitionwise_join](../t/try_partitionwise_join.md)
  - [apply_scanjoin_target_to_paths](apply_scanjoin_target_to_paths.md)
  - [adjust_appendrel_attrs_multilevel](adjust_appendrel_attrs_multilevel.md)

## Notes and Other Information
- Only works on post-sublink-conversion trees, avoiding recursion into sub-queries
- Includes assertions to prevent misuse with Query trees and ensure valid input parameters
- Similar in concept to pullup_replace_vars() but specialized for inheritance/partitioning scenarios
- Acts as a wrapper that establishes context before calling the actual mutator function

## Simplified Source

```c
Node *
adjust_appendrel_attrs(PlannerInfo *root, Node *node, int nappinfos,
                      AppendRelInfo **appinfos)
{
    adjust_appendrel_attrs_context context;

    // Set up transformation context
    context.root = root;
    context.nappinfos = nappinfos;
    context.appinfos = appinfos;

    // Validate input parameters
    Assert(nappinfos >= 1 && appinfos != NULL);
    Assert(node == NULL || !IsA(node, Query));

    // Perform the actual transformation using the mutator
    return adjust_appendrel_attrs_mutator(node, &context);
}
```