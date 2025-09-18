# find_recursive_union

## Location
[src/backend/utils/adt/ruleutils.c:5046-5075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L5046-L5075)

## Overview
Locates the ancestor RecursiveUnion plan node that generates the work table accessed by a given WorkTableScan by matching their shared wtParam identifier.

## Definition
```c
static Plan *find_recursive_union(deparse_namespace *dpns, WorkTableScan *wtscan)
```

## Detailed Description
This function traverses the ancestor plan node list within a deparse namespace to locate the RecursiveUnion plan node that corresponds to a given WorkTableScan. The relationship between these nodes is established through the wtParam field, which serves as a unique identifier within the plan tree for connecting WorkTableScan nodes to their generating RecursiveUnion operations.

The function performs a linear search through the ancestors list, checking each plan node to see if it is a RecursiveUnion with a matching wtParam value. This search is necessary because WorkTableScan nodes reference work tables created by RecursiveUnion operations, and during rule decompilation, the system needs to establish this connection to properly interpret variable references and generate correct SQL output.

If no matching RecursiveUnion is found, the function raises an error, as this indicates a corrupted or invalid plan tree structure where a WorkTableScan references a non-existent RecursiveUnion.

## Parameters / Member Variables
- `dpns`: Pointer to deparse_namespace containing the ancestor plan node list to search through
- `wtscan`: Pointer to the WorkTableScan node whose corresponding RecursiveUnion needs to be found

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace (namespace structure containing ancestor information)
  - WorkTableScan (work table scan plan node)
  - RecursiveUnion (recursive union plan node)
  - [Plan](../P/Plan.md) (base plan node structure)
  - ListCell (list cell structure for iteration)
  - lfirst (macro to extract data from list cell)
  - IsA (macro for type checking)
  - elog (error logging function)
- Called from (representative examples):
  - [set_deparse_plan](../s/set_deparse_plan.md) (at line 5010)

## Notes and Other Information
- This is a static function, only accessible within ruleutils.c
- The wtParam field serves as a unique identifier connecting WorkTableScan and RecursiveUnion nodes
- The function performs a fatal error if no matching RecursiveUnion is found, indicating plan tree corruption
- Essential for proper handling of recursive CTE (Common Table Expression) constructs during rule decompilation
- The linear search is acceptable because the number of ancestor nodes is typically small
- Part of the broader recursive query support infrastructure in PostgreSQL
- The function assumes that wtParam values are unique within the plan tree
- Used specifically in the context of rule decompilation where the plan tree structure needs to be analyzed and converted back to SQL text