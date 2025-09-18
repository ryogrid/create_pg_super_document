# relopt_enum_elt_def

## Location
src/include/access/reloptions.h: 117 - 121

## Overview
A structure that defines one member of the array of acceptable values for an enum reloption in PostgreSQL's relation options system.

## Definition


## Detailed Description
The `relopt_enum_elt_def` structure represents a single valid value for an enumerated relation option. It maps a string representation to an integer symbol value, allowing PostgreSQL to define enum-type relation options where users can specify values by name (string) which are internally represented as integers. This structure is fundamental to the relation options system for handling enumerated values like storage parameters.

## Parameters / Member Variables
- `string_val`: The string representation of the enum value that users can specify
- `symbol_val`: The corresponding integer value used internally to represent this enum option

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - init_enum_reloption (src/backend/access/common/reloptions.c:990)
  - add_enum_reloption (src/backend/access/common/reloptions.c:1019)
  - add_local_enum_reloption (src/backend/access/common/reloptions.c:1037)
  - parse_one_reloption (src/backend/access/common/reloptions.c:1652)
  - relopt_enum (src/include/access/reloptions.h:126)

## Notes and Other Information
This structure is typically used as part of an array to define all valid values for an enum relation option. The string_val provides the user-facing name while symbol_val provides the internal representation, enabling efficient storage and comparison while maintaining user-friendly interfaces.