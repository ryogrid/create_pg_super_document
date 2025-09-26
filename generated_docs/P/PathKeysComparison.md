# PathKeysComparison

## Location
[src/include/optimizer/paths.h:206-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/optimizer/paths.h#L206-L271)

## Overview
An enumeration type that represents the relationship between two sets of pathkeys, used by the PostgreSQL optimizer to determine ordering relationships and path selection strategies.

## Definition

```c
typedef enum
{
	PATHKEYS_EQUAL,				/* pathkeys are identical */
	PATHKEYS_BETTER1,			/* pathkey 1 is a superset of pathkey 2 */
	PATHKEYS_BETTER2,			/* vice versa */
	PATHKEYS_DIFFERENT,			/* neither pathkey includes the other */
} PathKeysComparison;
```
## Detailed Description
PathKeysComparison is a critical enumeration in PostgreSQL's query optimizer that encodes the relationship between two pathkey lists. Pathkeys represent the ordering properties of query execution paths, and this comparison result is essential for the optimizer to make informed decisions about which execution paths are preferable.

The enum values encode four distinct relationships:
- PATHKEYS_EQUAL indicates that two pathkey lists represent identical orderings
- PATHKEYS_BETTER1 means the first pathkey list is a superset of the second (more ordering information)
- PATHKEYS_BETTER2 means the second pathkey list is a superset of the first
- PATHKEYS_DIFFERENT indicates neither pathkey list subsumes the other

This comparison is fundamental to path selection algorithms, as paths with "better" pathkeys (more specific ordering) can often eliminate the need for additional sorting operations.

## Parameters / Member Variables
- : Two pathkey lists are identical, representing the same ordering
- : The first pathkey list is a superset of the second, providing more complete ordering information
- : The second pathkey list is a superset of the first, providing more complete ordering information
- : Neither pathkey list includes the other; they represent incompatible orderings

## Dependencies
- Functions called/Symbols referenced:
  - Used as return type for 
  - Referenced in pathkey utility functions
- Called from (representative examples):
  -  (src/backend/optimizer/util/pathnode.c:446)
  -  (src/backend/optimizer/util/pathnode.c:659)
  -  (src/backend/optimizer/util/pathnode.c:770)
  -  (src/backend/optimizer/util/pathnode.c:884)

## Notes and Other Information
- The comparison assumes pathkeys are canonical, allowing simple pointer comparisons for equality
- This enum is central to PostgreSQL's cost-based optimizer decision making
- The "better" relationship is used to determine when one execution path can subsume another's ordering requirements
- Defined in src/include/optimizer/paths.h:200-206
- Closely tied to the PathKey data structure and equivalence class system