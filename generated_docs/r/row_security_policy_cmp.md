# row_security_policy_cmp

## Location
src/backend/rewrite/rowsecurity.c: 674 - 699

## Overview
This static comparison function enables sorting of RowSecurityPolicy structures by their policy names, with special handling for NULL policy names from extensions.

## Definition


## Detailed Description
The  function serves as a comparator for the  function when sorting row-level security policies by name. It extracts RowSecurityPolicy structures from ListCell containers and compares their policy names using standard string comparison. The function includes defensive programming by handling the edge case where extension-provided policies might have NULL policy names.

The comparison follows standard C library conventions: returning a negative value if the first policy should come before the second, zero if they are equal, and a positive value if the first should come after the second. NULL policy names are treated as greater than any non-NULL name, ensuring they sort to the end of the list.

## Parameters / Member Variables
- : ListCell containing the first RowSecurityPolicy to compare
- : ListCell containing the second RowSecurityPolicy to compare

## Dependencies
- Functions called/Symbols referenced:
  - RowSecurityPolicy (structure access)
  - strcmp (standard library function)
- Called from (representative examples):
  - sort_policies_by_name (via list_sort)

## Notes and Other Information
- Designed specifically for use with PostgreSQL's list_sort function
- Includes NULL safety for policy names, which can occur with extension-provided policies
- Uses standard lexicographic string comparison via strcmp
- NULL policy names are treated as greater than any non-NULL name for consistent ordering
- The function follows the standard comparator contract expected by sorting algorithms
- Policy name comparison is case-sensitive following strcmp behavior