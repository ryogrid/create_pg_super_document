# DummyIndexOptions

## Location
[src/test/modules/dummy_index_am/dummy_index_am.c:39-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/dummy_index_am/dummy_index_am.c#L39-L48)

## Overview
DummyIndexOptions is a struct that defines the reloption parameters for the dummy index access method, serving as a test template for custom index access method development.

## Definition

```c
typedef struct DummyIndexOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			option_int;
	double		option_real;
	bool		option_bool;
	DummyAmEnum option_enum;
	int			option_string_val_offset;
	int			option_string_null_offset;
}			DummyIndexOptions;
```
## Detailed Description
DummyIndexOptions is a structure used in PostgreSQL's dummy index access method test module to demonstrate how custom index access methods can define and handle reloptions (relation options). This struct serves as a template showing how different data types (integer, real, boolean, enum, and string) can be incorporated as configurable options for an index access method. The structure follows PostgreSQL's varlena format with a header, making it suitable for storage and retrieval within the relation options system.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header required for PostgreSQL's variable-length data structures (should not be modified directly)
- `option_int`: Integer option parameter with default value 10, range -10 to 100
- `option_real`: Real (double) option parameter with default value 3.1415, range -10 to 100
- `option_bool`: Boolean option parameter with default value true
- `option_enum`: Enumeration option parameter of type DummyAmEnum with default DUMMY_AM_ENUM_ONE (values "one" or "two")
- `option_string_val_offset`: Offset for string option with non-NULL default value "DefaultValue"
- `option_string_null_offset`: Offset for string option with NULL default value
## Dependencies
- Functions called/Symbols referenced:
  - DummyAmEnum
- Called from (representative examples):
  - [create_reloptions_table](../c/create_reloptions_table.md) (multiple references for offsetof calculations)
  - diocptions function

## Notes and Other Information
- This struct is part of the dummy_index_am test module located in src/test/modules/dummy_index_am/
- It demonstrates the proper way to structure reloptions for custom index access methods
- The string options use offset fields rather than direct string storage, following PostgreSQL's pattern for variable-length data
- All option types supported by PostgreSQL's reloption system are represented: int, real, bool, enum, and string
- The structure is used with offsetof() macro to register field offsets with the reloption system
- Serves as a template that developers can reference when implementing custom index access methods