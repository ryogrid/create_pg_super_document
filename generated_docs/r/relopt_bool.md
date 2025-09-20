# relopt_bool

## Location
[src/include/access/reloptions.h:91-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/reloptions.h#L91-L95)

## Overview
relopt_bool is a structure that defines a boolean-type relation option, extending the base relopt_gen structure with a default boolean value.

## Definition

```c
typedef struct relopt_bool
{
	relopt_gen	gen;
	bool		default_val;
} relopt_bool;
```
## Detailed Description
The relopt_bool structure represents a boolean relation option definition in PostgreSQL's reloption system. It inherits all the common metadata from relopt_gen (name, description, applicable object kinds, etc.) and adds a default_val field to specify the default boolean value for this option.

This structure is used to define boolean options that can be set on database objects like tables, indexes, and other relations. Examples might include options like "autovacuum_enabled" or "toast.compression" that have true/false semantics.

## Parameters / Member Variables
- : The base relopt_gen structure containing common option metadata (name, description, kinds, lockmode, namelen, type)
- : The default boolean value to use when this option is not explicitly specified by the user

## Dependencies
- Functions called/Symbols referenced:
  - [relopt_gen](relopt_gen.md)
- Called from (representative examples):
  - [allocate_reloption](../a/allocate_reloption.md)
  - [init_bool_reloption](../i/init_bool_reloption.md)
  - [add_bool_reloption](../a/add_bool_reloption.md)
  - [add_local_bool_reloption](../a/add_local_bool_reloption.md)
  - [fillRelOptions](../f/fillRelOptions.md)

## Notes and Other Information
This is one of the type-specific structures in the relation options system. The gen field must be the first member to allow casting between relopt_bool* and relopt_gen* for polymorphic handling. The type field in the embedded gen structure will be set to RELOPT_TYPE_BOOL for all instances of this structure.