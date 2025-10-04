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

## Simplified Source

```c
static void
create_reloptions_table(void)
{
    // Create relation option kind for dummy index AM
    di_relopt_kind = add_reloption_kind();

    // Add integer option: option_int (default 10, range -10 to 100)
    add_int_reloption(di_relopt_kind, "option_int", "Integer option for dummy_index_am",
                      10, -10, 100, AccessExclusiveLock);
    di_relopt_tab[0] = (relopt_parse_elt) {"option_int", RELOPT_TYPE_INT,
                                           offsetof(DummyIndexOptions, option_int)};

    // Add real option: option_real (default 3.1415, range -10 to 100)
    add_real_reloption(di_relopt_kind, "option_real", "Real option for dummy_index_am",
                       3.1415, -10, 100, AccessExclusiveLock);
    di_relopt_tab[1] = (relopt_parse_elt) {"option_real", RELOPT_TYPE_REAL,
                                           offsetof(DummyIndexOptions, option_real)};

    // Add boolean option: option_bool (default true)
    add_bool_reloption(di_relopt_kind, "option_bool", "Boolean option for dummy_index_am",
                       true, AccessExclusiveLock);
    di_relopt_tab[2] = (relopt_parse_elt) {"option_bool", RELOPT_TYPE_BOOL,
                                           offsetof(DummyIndexOptions, option_bool)};

    // Add enum option: option_enum (values "one", "two", default "one")
    add_enum_reloption(di_relopt_kind, "option_enum", "Enum option for dummy_index_am",
                       dummyAmEnumValues, DUMMY_AM_ENUM_ONE,
                       "Valid values are \"one\" and \"two\".", AccessExclusiveLock);
    di_relopt_tab[3] = (relopt_parse_elt) {"option_enum", RELOPT_TYPE_ENUM,
                                           offsetof(DummyIndexOptions, option_enum)};

    // Add string options with and without defaults
    add_string_reloption(di_relopt_kind, "option_string_val",
                         "String option for dummy_index_am with non-NULL default",
                         "DefaultValue", &validate_string_option, AccessExclusiveLock);
    di_relopt_tab[4] = (relopt_parse_elt) {"option_string_val", RELOPT_TYPE_STRING,
                                           offsetof(DummyIndexOptions, option_string_val_offset)};

    add_string_reloption(di_relopt_kind, "option_string_null", NULL,
                         NULL, &validate_string_option, AccessExclusiveLock);
    di_relopt_tab[5] = (relopt_parse_elt) {"option_string_null", RELOPT_TYPE_STRING,
                                           offsetof(DummyIndexOptions, option_string_null_offset)};
}
```