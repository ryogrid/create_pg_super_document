# list_head

## Location
[src/include/nodes/pg_list.h:128-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pg_list.h#L128-L134)

## Overview
Returns the first cell in a PostgreSQL list structure, or NULL if the list is empty.

## Definition

```c
static inline ListCell *
list_head(const List *l)
```
## Detailed Description
The list_head function is a small inline utility function that provides access to the first cell of a PostgreSQL List structure. It safely handles NULL lists by returning NULL rather than causing a segmentation fault. This function is designed to be inline due to its frequent usage throughout the PostgreSQL codebase and its simple implementation.

The function operates on PostgreSQL's internal List data structure, which stores elements as an array of ListCell objects. By returning the first element of this array (l->elements[0]), it provides access to the head of the list.

## Parameters / Member Variables
- : A const pointer to the List structure. Can be NULL, in which case the function returns NULL.

## Dependencies
- Functions called/Symbols referenced: None (simple array access)
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md) (src/backend/access/common/printtup.c:171)
  - [ConstructTupleDescriptor](../C/ConstructTupleDescriptor.md) (src/backend/catalog/index.c:289-290)
  - [FormIndexDatum](../F/FormIndexDatum.md) (src/backend/catalog/index.c:2720)
  - [NameListToString](../N/NameListToString.md) (src/backend/catalog/namespace.c:3605)
  - [do_analyze_rel](../d/do_analyze_rel.md) (src/backend/commands/analyze.c:452)
  - [create_mergejoin_plan](../c/create_mergejoin_plan.md) (src/backend/optimizer/plan/createplan.c:4590-4591)
  - [transformUpdateTargetList](../t/transformUpdateTargetList.md) (src/backend/parser/analyze.c:2501)

## Notes and Other Information
- This function is marked as static inline for performance optimization due to its frequent usage
- Part of the PostgreSQL list manipulation API defined in src/include/nodes/pg_list.h
- Safely handles NULL input lists without crashing
- Used extensively throughout the PostgreSQL codebase for list traversal and element access
- The function assumes that if the list is not NULL, it contains at least the elements array structure