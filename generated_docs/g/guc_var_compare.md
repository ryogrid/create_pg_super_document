# guc_var_compare

## Location
[src/backend/utils/misc/guc.c:1290-1301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L1290-L1301)

## Overview
A comparison function used for sorting GUC (Grand Unified Configuration) variables by their names when ordering an array of GUC pointers.

## Definition

```c
static int
guc_var_compare(const void *a, const void *b)
```
## Detailed Description
This function serves as a comparator for qsort operations on arrays of GUC variable pointers. It extracts the variable names from two GUC structure pointers and delegates the actual comparison to the  function. The function follows the standard qsort comparator interface, taking two void pointers that represent GUC variables and returning an integer indicating their relative order.

The function handles the complex pointer dereferencing needed to access GUC variable names from an array of pointers to GUC structures. It performs double dereferencing to get to the actual name strings stored within the GUC configuration structures.

## Parameters / Member Variables
- `*a`: Pointer to the first GUC variable pointer in the comparison
- `*b`: Pointer to the second GUC variable pointer in the comparison
## Dependencies
- Functions called/Symbols referenced:
  - [guc_name_compare](guc_name_compare.md)
- Called from (representative examples):
  - [get_guc_variables](get_guc_variables.md) (used as qsort comparator)

## Notes and Other Information
- This is a static function internal to the GUC system implementation
- Used specifically for sorting arrays of GUC variables by name for display or search purposes
- The complex pointer dereferencing pattern  is necessary due to the structure of GUC variable arrays
- Location: src/backend/utils/misc/guc.c:1290-1301

## Simplified Source

```c
static int guc_var_compare(const void *a, const void *b) {
    // Extract variable names from GUC structure pointers
    const char *namea = **(const char **const *) a;
    const char *nameb = **(const char **const *) b;

    // Compare names using GUC-specific comparison
    return guc_name_compare(namea, nameb);
}
```