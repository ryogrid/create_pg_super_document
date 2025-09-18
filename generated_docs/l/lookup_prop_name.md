# lookup_prop_name

## Location
src/backend/utils/adt/amutils.c: 90 - 116

## Overview
Converts a string property name to its corresponding IndexAMProperty enum value for efficient access method property lookup.

## Definition


## Detailed Description
This function performs a case-insensitive lookup of property names in the predefined am_propnames array to convert string-based property names into their corresponding IndexAMProperty enumeration values. The function supports all standard index access method properties such as ordering capabilities, scan types, and structural features. If the property name is not found in the standard list, it returns AMPROP_UNKNOWN rather than throwing an error, allowing individual access methods to define their own custom properties.

## Parameters / Member Variables
- : The string name of the index access method property to look up (case-insensitive)

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array length)
  - pg_strcasecmp (case-insensitive string comparison)
  - AMPROP_UNKNOWN (enum value for unknown properties)
- Called from (representative examples):
  - indexam_property

## Notes and Other Information
- The function is static and only used within amutils.c
- Supports standard properties like 'asc', 'desc', 'nulls_first', 'nulls_last', 'orderable', 'distance_orderable', 'returnable', 'search_array', 'search_nulls', 'clusterable', 'index_scan', 'bitmap_scan', 'backward_scan', 'can_order', 'can_unique', 'can_multi_col', 'can_exclude', and 'can_include'
- Returns AMPROP_UNKNOWN for unrecognized properties instead of throwing an error, allowing extensibility for custom access methods
- Uses case-insensitive comparison for property name matching