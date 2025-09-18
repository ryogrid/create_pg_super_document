# relopt_value

## Location
[src/include/access/reloptions.h:76-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/reloptions.h#L76-L88)

## Overview
relopt_value is a structure that holds a parsed relation option value, containing both the option metadata and the actual value in a type-safe union.

## Definition


## Detailed Description
The relopt_value structure represents a parsed and validated relation option value. It serves as a container that links the option definition (via the gen pointer) with its actual value stored in a type-safe union. This structure is used throughout the relation options parsing and processing pipeline to maintain both the metadata about an option and its concrete value.

The union design allows for efficient storage of different value types while maintaining type safety through the relopt_gen's type field. The isset flag indicates whether this option was explicitly set by the user or should use its default value.

## Parameters / Member Variables
- : Pointer to the relopt_gen structure that defines this option's metadata (name, type, constraints, etc.)
- : Boolean flag indicating whether this option was explicitly provided by the user (true) or should use its default value (false)
- : Union containing the actual option value in the appropriate type:
  - : Boolean value for RELOPT_TYPE_BOOL options
  - : Integer value for RELOPT_TYPE_INT options  
  - : Floating-point value for RELOPT_TYPE_REAL options
  - : Integer representing the enum choice for RELOPT_TYPE_ENUM options
  - : Null-terminated string for RELOPT_TYPE_STRING options (memory allocated separately)

## Dependencies
- Functions called/Symbols referenced:
  - [relopt_gen](relopt_gen.md)
- Called from (representative examples):
  - [parseRelOptionsInternal](../p/parseRelOptionsInternal.md)
  - [parseRelOptions](../p/parseRelOptions.md)
  - [parse_one_reloption](../p/parse_one_reloption.md)
  - [allocateReloptStruct](../a/allocateReloptStruct.md)
  - [fillRelOptions](../f/fillRelOptions.md)

## Notes and Other Information
This structure is central to the relation options processing workflow, serving as the bridge between raw option strings and typed option values. The string_val member requires separate memory allocation and deallocation. The structure is typically used in arrays when processing multiple options simultaneously, with the isset flag helping distinguish between user-provided and default values.