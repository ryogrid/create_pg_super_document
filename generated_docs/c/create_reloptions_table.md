# create_reloptions_table

## Location
[src/test/modules/dummy_index_am/dummy_index_am.c:76-138](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/dummy_index_am/dummy_index_am.c#L76-L138)

## Overview
Creates a comprehensive set of relation option types for the dummy index access method, demonstrating various option patterns and types.

## Definition
```c
static void create_reloptions_table(void)
```

## Detailed Description
This function sets up a complete relation options table for PostgreSQL's dummy index access method test module. It demonstrates how to register different types of relation options including integer, real, boolean, enum, and string options. The function creates both the option definitions using PostgreSQL's relation option API and populates a corresponding table structure (di_relopt_tab) with metadata about each option including names, types, and memory offsets.

The function showcases various option patterns:
- Integer options with min/max bounds
- Real number options with default values and bounds
- Boolean options with default values
- Enumeration options with predefined valid values
- String options with both NULL and non-NULL defaults, including custom validation

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [add_reloption_kind](../a/add_reloption_kind.md) (creates a new relation option kind)
  - [add_int_reloption](../a/add_int_reloption.md) (registers integer option)
  - [add_real_reloption](../a/add_real_reloption.md) (registers real number option)
  - [add_bool_reloption](../a/add_bool_reloption.md) (registers boolean option)
  - [add_enum_reloption](../a/add_enum_reloption.md) (registers enumeration option)
  - [add_string_reloption](../a/add_string_reloption.md) (registers string option)
  - [validate_string_option](../v/validate_string_option.md) (custom validation function)
  - offsetof (macro for structure member offset)
  - AccessExclusiveLock (lock level constant)
  - Various RELOPT_TYPE_* constants (option type identifiers)
  - [DummyIndexOptions](../D/DummyIndexOptions.md) (structure type for option storage)
  - DUMMY_AM_ENUM_ONE (default enum value)
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (module initialization function at src/test/modules/dummy_index_am/dummy_index_am.c:332)

## Notes and Other Information
- This function is part of PostgreSQL's test infrastructure for demonstrating relation option handling
- The function is declared as static, limiting its scope to the compilation unit
- Creates 6 different option types: option_int, option_real, option_bool, option_enum, option_string_val, and option_string_null
- Uses AccessExclusiveLock for all options, meaning they require exclusive access to modify
- The di_relopt_tab array is populated with metadata for each option to facilitate option processing
- Located in src/test/modules/dummy_index_am/dummy_index_am.c:76-138
- Serves as a comprehensive example for developers implementing custom relation options