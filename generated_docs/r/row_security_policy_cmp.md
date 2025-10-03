# row_security_policy_cmp

## Location
[src/backend/rewrite/rowsecurity.c:674-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rowsecurity.c#L674-L699)

## Overview
This static comparison function enables sorting of RowSecurityPolicy structures by their policy names, with special handling for NULL policy names from extensions.

## Definition

```c
static int
row_security_policy_cmp(const ListCell *a, const ListCell *b)
```
## Detailed Description
The  function serves as a comparator for the  function when sorting row-level security policies by name. It extracts RowSecurityPolicy structures from ListCell containers and compares their policy names using standard string comparison. The function includes defensive programming by handling the edge case where extension-provided policies might have NULL policy names.

The comparison follows standard C library conventions: returning a negative value if the first policy should come before the second, zero if they are equal, and a positive value if the first should come after the second. NULL policy names are treated as greater than any non-NULL name, ensuring they sort to the end of the list.

## Parameters / Member Variables
- `*a`: ListCell containing the first RowSecurityPolicy to compare
- `*b`: ListCell containing the second RowSecurityPolicy to compare
## Dependencies
- Functions called/Symbols referenced:
  - [RowSecurityPolicy](../R/RowSecurityPolicy.md) (structure access)
  - strcmp (standard library function)
- Called from (representative examples):
  - [sort_policies_by_name](../s/sort_policies_by_name.md) (via list_sort)

## Notes and Other Information
- Designed specifically for use with PostgreSQL's list_sort function
- Includes NULL safety for policy names, which can occur with extension-provided policies
- Uses standard lexicographic string comparison via strcmp
- NULL policy names are treated as greater than any non-NULL name for consistent ordering
- The function follows the standard comparator contract expected by sorting algorithms
- Policy name comparison is case-sensitive following strcmp behavior

## Simplified Source

```c
static int row_security_policy_cmp(const ListCell *a, const ListCell *b) {
    const RowSecurityPolicy *pa = (const RowSecurityPolicy *) lfirst(a);
    const RowSecurityPolicy *pb = (const RowSecurityPolicy *) lfirst(b);

    // Handle NULL policy names from extensions
    if (pa->policy_name == NULL)
        return pb->policy_name == NULL ? 0 : 1;
    if (pb->policy_name == NULL)
        return -1;

    // Standard string comparison
    return strcmp(pa->policy_name, pb->policy_name);
}
```