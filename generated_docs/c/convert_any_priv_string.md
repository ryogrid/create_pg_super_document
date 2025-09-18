# convert_any_priv_string

## Location
src/backend/utils/adt/acl.c: 1687 - 1734

## Overview
Parses a comma-separated string of privilege names and converts them to a bitmask using a provided privilege mapping table.

## Definition
```c
static AclMode convert_any_priv_string(text *priv_type_text, const priv_map *privileges)
```

## Detailed Description
The `convert_any_priv_string` function serves as a generic privilege string parser for PostgreSQL's ACL system. It takes a text string containing comma-separated privilege names and converts them into a corresponding AclMode bitmask using a provided mapping table. The function is designed to be liberal with whitespace between privilege names but strict about the names themselves.

The function operates by:
1. Converting the input text to a C string
2. Splitting the string on commas
3. Trimming whitespace from each privilege name
4. Looking up each name in the provided privilege mapping table using case-insensitive comparison
5. OR'ing together the privilege bits for all recognized names
6. Raising an error if any unrecognized privilege name is encountered

This function is the core parsing engine used by all the specific privilege conversion functions in PostgreSQL's ACL system.

## Parameters / Member Variables
- `priv_type_text`: Text string containing comma-separated privilege names to parse
- `privileges`: Array of priv_map structures defining valid privilege names and their corresponding bit values, terminated by a NULL name entry

## Dependencies
- Functions called/Symbols referenced:
  - text_to_cstring (converts PostgreSQL text to C string)
  - strchr (finds comma separators)
  - isspace (checks for whitespace characters)
  - strlen (gets string length)
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (case-insensitive string comparison)
  - ereport/ERROR (error reporting)
  - [errcode](../e/errcode.md)/ERRCODE_INVALID_PARAMETER_VALUE (error code)
  - [errmsg](../e/errmsg.md) (error message formatting)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
- Called from (representative examples):
  - [makeaclitem](../m/makeaclitem.md) (creates ACL items from components)
  - [convert_table_priv_string](convert_table_priv_string.md) (converts table privilege strings)
  - [convert_sequence_priv_string](convert_sequence_priv_string.md) (converts sequence privilege strings)
  - [convert_column_priv_string](convert_column_priv_string.md) (converts column privilege strings)
  - [convert_database_priv_string](convert_database_priv_string.md) (converts database privilege strings)
  - Multiple other convert_*_priv_string functions for different object types

## Notes and Other Information
- This is a static function used internally within the ACL module
- Performs case-insensitive privilege name matching for user convenience
- Handles whitespace liberally between privilege names but not within names
- Throws an error for any unrecognized privilege name to ensure strict validation
- Uses OR operation to combine multiple privilege bits into a single bitmask
- Memory management: allocates temporary string copy and cleans it up
- Core utility function that enables consistent privilege parsing across all PostgreSQL object types