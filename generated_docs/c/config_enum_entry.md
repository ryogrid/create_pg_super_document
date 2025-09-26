# config_enum_entry

## Location
[src/include/utils/guc.h:169-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/guc.h#L169-L198)

## Overview
config_enum_entry is a structure that defines the possible values for enum-type configuration variables in PostgreSQL's GUC system, mapping human-readable names to internal integer values.

## Definition

```c
struct config_enum_entry
{
	const char *name;
	int			val;
	bool		hidden;
};
```
## Detailed Description
The config_enum_entry structure is used to define the allowable values for enum-type configuration variables in PostgreSQL's Grand Unified Configuration (GUC) system. Each enum configuration variable is associated with an array of these entries, where each entry maps a human-readable string name to an internal integer value.

This structure enables PostgreSQL to provide user-friendly string-based configuration while internally working with efficient integer values. For example, a logging level configuration might map "debug" to 5, "info" to 4, etc.

The hidden flag provides a mechanism to accept certain values for backward compatibility or internal use while not exposing them to users when they request a list of valid options. This allows for deprecated or special-purpose values to remain functional without cluttering user documentation.

## Parameters / Member Variables
- : The human-readable string name for this enum value (e.g., "on", "off", "debug")
- : The internal integer value that corresponds to this name
- : Flag indicating whether this value should be hidden from user-visible option lists (true = hidden from display but still accepted)

## Dependencies
- Functions called/Symbols referenced:
  - GucSource (referenced in nearby code sections)
- Called from (representative examples):
  - [config_enum_lookup_by_value](config_enum_lookup_by_value.md)
  - [config_enum_lookup_by_name](config_enum_lookup_by_name.md)
  - [config_enum_get_options](config_enum_get_options.md)
  - [DefineCustomEnumVariable](../D/DefineCustomEnumVariable.md)
  - Various enum configuration tables in guc_tables.c

## Notes and Other Information
- Used extensively in guc_tables.c to define enum options for built-in configuration variables
- Arrays of config_enum_entry structures are terminated by a NULL name entry
- The hidden flag allows for backward compatibility - deprecated values can still be accepted but won't appear in help text
- Essential for translating between user-friendly configuration syntax and internal representations
- Supports PostgreSQL's philosophy of providing both usability and performance optimization
- Related to the broader enum configuration infrastructure including config_enum structure type