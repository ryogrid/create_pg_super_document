# list_next_fn

## Location
[src/backend/optimizer/util/predtest.c:915-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L915-L927)

## Overview
A specialized iteration function that retrieves the next element from a PostgreSQL List during predicate analysis iteration.

## Definition

```c
static Node *
list_next_fn(PredIterInfo info)
{
	ListCell   *l = (ListCell *) info->state;
	Node	   *n;

	if (l == NULL)
		return NULL;
	n = lfirst(l);
	info->state = (void *) lnext(info->state_list, l);
	return n;
}
```
## Detailed Description
This function implements the "next" operation for iterating over regular PostgreSQL Lists within the predicate iteration framework. It retrieves the current node from the iteration state, advances the iterator to the next ListCell, and returns the current node. When the end of the list is reached, it returns NULL to signal completion. This function works in conjunction with list_startup_fn and list_cleanup_fn to provide a complete iteration interface.

## Parameters
- `info`: A PredIterInfo structure containing the current iteration state, including the current ListCell position

## Dependencies
- Functions called/Symbols referenced:
  - [PredIterInfo](../P/PredIterInfo.md) (structure type)
  - lfirst (macro to get the data from a ListCell)
  - [lnext](lnext.md) (function to get the next ListCell in the list)
- Called from (representative examples):
  - iterate_end (during predicate classification)
  - [predicate_classify](../p/predicate_classify.md) (multiple locations in predicate analysis logic)

## Notes and Other Information
- Returns NULL when the end of the list is reached, indicating iteration completion
- The function advances the iteration state before returning the current node
- Uses PostgreSQL's standard list manipulation macros (lfirst, lnext)
- The function is stateful - each call modifies the iterator position stored in info->state
- This is a static function used internally within the predicate testing module
- Part of the function pointer-based iteration pattern that allows uniform handling of different node types

## Simplified Source

```c
static Node *
list_next_fn(PredIterInfo info)
{
    ListCell *current = (ListCell *) info->state;

    // Return NULL if at end of list
    if (current == NULL)
        return NULL;

    // Get current node and advance to next position
    Node *node = lfirst(current);
    info->state = (void *) lnext(info->state_list, current);

    return node;
}
```