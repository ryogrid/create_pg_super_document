# contain_placeholder_references_context

## Location
[src/backend/optimizer/util/placeholder.c:27-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L27-L31)

## Overview
A context structure used during tree walking to detect whether PlaceHolderVars contain references to a specific relation ID in PostgreSQL's query optimizer.

## Definition

```c
typedef struct contain_placeholder_references_context
{
	int			relid;
	int			sublevels_up;
} contain_placeholder_references_context;
```
## Detailed Description
This structure serves as a context parameter for the tree walker function  when searching for PlaceHolderVar references to a specific relation. It's used in the process of determining whether changing the nullability status of a relation might affect what a PlaceHolderVar computes. The structure maintains state during recursive traversal of expression trees and query trees to track the target relation ID and the current nesting level of subqueries.

## Parameters / Member Variables
- `relid`: The relation ID (typically an outer join relation ID) to search for within PlaceHolderVar expressions
- `sublevels_up`: Counter tracking the current nesting level when recursing into subqueries; used to match against PlaceHolderVar's phlevelsup field
## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - [contain_placeholder_references_to](contain_placeholder_references_to.md)
  - [contain_placeholder_references_walker](contain_placeholder_references_walker.md)

## Notes and Other Information
- This context structure is specifically designed for the placeholder reference detection mechanism in PostgreSQL's optimizer
- The  field is crucial for correctly handling nested subqueries, ensuring that PlaceHolderVars are examined at the appropriate query level
- Used primarily in outer join processing where determining placeholder variable dependencies on specific relations is important for correctness
- The structure is defined in src/backend/optimizer/util/placeholder.c:27-31