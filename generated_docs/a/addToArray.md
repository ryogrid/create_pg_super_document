# addToArray

## Location
[src/backend/utils/misc/tzparser.c:188-275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/tzparser.c#L188-L275)

## Overview
Inserts a timezone entry into a sorted array while maintaining alphabetical order and handling duplicate entries.

## Definition
static int addToArray(tzEntry **base, int *arraysize, int n, tzEntry *entry, bool override)

## Detailed Description
This function maintains a dynamically sized array of timezone entries in sorted order by abbreviation name. It uses binary search to locate the correct insertion position and handles duplicate entries according to the override parameter. When duplicates are found, it either preserves identical entries, overrides differing entries if allowed, or reports conflicts. The array is automatically resized when needed using repalloc.

## Parameters / Member Variables
- : Base address of the array (changeable if array must be enlarged)
- : Allocated length of array (changeable if array must be enlarged)  
- : Current number of valid elements in the array
- : New timezone entry data to insert
- : True if OK to override existing entries with same abbreviation

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - GUC_check_errmsg
  - GUC_check_errdetail
  - [repalloc](../r/repalloc.md)
  - memmove
  - memcpy
- Called from (representative examples):
  - [ParseTzFile](../P/ParseTzFile.md)

## Notes and Other Information
The function returns the new array length on success, or -1 on error. It uses strcmp() to ensure the sort order matches what datetime.c expects. Duplicate checking considers both the abbreviation and the associated timezone data (offset, zone name, DST flag) to determine if entries are truly identical.

## Simplified Source

```c
static int addToArray(tzEntry **base, int *arraysize, int n,
                     tzEntry *entry, bool override) {
    tzEntry *arrayptr = *base;
    int low = 0, high = n - 1;

    // Binary search to find insertion position or duplicate
    while (low <= high) {
        int mid = (low + high) >> 1;
        tzEntry *midptr = arrayptr + mid;
        int cmp = strcmp(entry->abbrev, midptr->abbrev);

        if (cmp < 0) {
            high = mid - 1;
        } else if (cmp > 0) {
            low = mid + 1;
        } else {
            // Found duplicate - check if entries are identical
            bool identical = (midptr->zone == NULL && entry->zone == NULL &&
                             midptr->offset == entry->offset &&
                             midptr->is_dst == entry->is_dst) ||
                            (midptr->zone != NULL && entry->zone != NULL &&
                             strcmp(midptr->zone, entry->zone) == 0);

            if (identical) return n;  // No change needed

            if (override) {
                // Override existing entry
                midptr->zone = entry->zone;
                midptr->offset = entry->offset;
                midptr->is_dst = entry->is_dst;
                return n;
            }

            // Conflict - report error
            GUC_check_errmsg("time zone abbreviation \"%s\" is multiply defined",
                            entry->abbrev);
            return -1;
        }
    }

    // Enlarge array if needed
    if (n >= *arraysize) {
        *arraysize *= 2;
        *base = (tzEntry *) repalloc(*base, *arraysize * sizeof(tzEntry));
    }

    // Insert new entry at position 'low'
    arrayptr = *base + low;
    memmove(arrayptr + 1, arrayptr, (n - low) * sizeof(tzEntry));
    memcpy(arrayptr, entry, sizeof(tzEntry));

    return n + 1;
}
```