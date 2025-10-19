# lookup_prop_name

## Location
[src/backend/utils/adt/amutils.c:90-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/amutils.c#L90-L116)

## Overview
Converts a string property name to its corresponding IndexAMProperty enum value for efficient access method property lookup.

## Definition

```c
static IndexAMProperty
lookup_prop_name(const char *name)
```
## Detailed Description
This function performs a case-insensitive lookup of property names in the predefined am_propnames array to convert string-based property names into their corresponding IndexAMProperty enumeration values. The function supports all standard index access method properties such as ordering capabilities, scan types, and structural features. If the property name is not found in the standard list, it returns AMPROP_UNKNOWN rather than throwing an error, allowing individual access methods to define their own custom properties.

## Parameters / Member Variables
- `*name`: The string name of the index access method property to look up (case-insensitive)
## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - AMPROP_UNKNOWN (enum value for unknown properties)
- Called from (representative examples):
  - [indexam_property](../i/indexam_property.md)

## Notes and Other Information
- The function is static and only used within amutils.c
- Supports standard properties like 'asc', 'desc', 'nulls_first', 'nulls_last', 'orderable', 'distance_orderable', 'returnable', 'search_array', 'search_nulls', 'clusterable', 'index_scan', 'bitmap_scan', 'backward_scan', 'can_order', 'can_unique', 'can_multi_col', 'can_exclude', and 'can_include'
- Returns AMPROP_UNKNOWN for unrecognized properties instead of throwing an error, allowing extensibility for custom access methods
- Uses case-insensitive comparison for property name matching

## Simplified Source

```c
static IndexAMProperty
lookup_prop_name(const char *name)
{
    // Search through the property names array
    for (int i = 0; i < lengthof(am_propnames); i++)
    {
        // Case-insensitive comparison with property name
        if (pg_strcasecmp(am_propnames[i].name, name) == 0)
            return am_propnames[i].prop;
    }

    // Return unknown if property not found (allows custom AM properties)
    return AMPROP_UNKNOWN;
}
```