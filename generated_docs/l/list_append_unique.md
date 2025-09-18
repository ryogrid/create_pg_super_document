# list_append_unique

## Location
[src/backend/nodes/list.c:1343-1355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/list.c#L1343-L1355)

## Overview
Appends a datum to a list only if it is not already present in the list, using deep comparison via the equal() function to determine membership.

## Definition
```c
List *list_append_unique(List *list, void *datum)
```

## Detailed Description
This function provides a convenient way to append an element to a list while ensuring no duplicates are created. Before appending the datum, it first checks whether the datum is already present in the list using deep comparison via the `equal()` function. If the datum is found to be a member, the original list is returned unchanged. If the datum is not found, it is appended to the list using `lappend()`.

The function performs a simple linear search through the list to check for membership, which makes it inefficient for long lists. It is most suitable for maintaining small lists where uniqueness is required.

## Parameters / Member Variables
- `list`: The target list to append to (can be NIL)
- `datum`: The data element to append if not already present

## Dependencies
- Functions called/Symbols referenced:
  - [list_member](list_member.md) - Checks if datum is already in the list using equal() comparison
  - `lappend` - Appends the datum to the list if not already present
- Called from (representative examples):
  - [check_publications_origin](../c/check_publications_origin.md) (src/backend/commands/subscriptioncmds.c:2095)
  - [create_index_paths](../c/create_index_paths.md) (src/backend/optimizer/path/indxpath.c:371)
  - [add_security_quals](../a/add_security_quals.md) (src/backend/rewrite/rowsecurity.c:750, 765)
  - `QUAL_FOR_WCO` (src/backend/rewrite/rowsecurity.c:860, 885)

## Notes and Other Information
- Uses deep comparison via `equal()` function to determine membership
- Performs a linear search, making it inefficient for long lists
- Returns the original list pointer if datum is already present
- Returns a new list pointer if datum was appended
- The list can be NIL (empty) - in this case, a new single-element list is created
- Suitable for maintaining small lists where duplicate prevention is important
- For better performance with large lists, consider using hash-based uniqueness checking