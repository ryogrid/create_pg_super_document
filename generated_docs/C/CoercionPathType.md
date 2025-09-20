# CoercionPathType

## Location
[src/include/parser/parse_coerce.h:31-105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_coerce.h#L31-L105)

## Overview
CoercionPathType is an enumeration that defines result codes returned by the find_coercion_pathway function to indicate different types of type coercion pathways available in PostgreSQL's type system.

## Definition

```c
structName);
```
## Detailed Description
CoercionPathType represents the different strategies PostgreSQL can use to convert values from one data type to another. The enum is primarily used by the type coercion system to communicate what kind of conversion mechanism should be employed when transforming data types. Each enum value corresponds to a specific coercion strategy:

- **COERCION_PATH_NONE**: No coercion pathway could be found between the source and target types
- **COERCION_PATH_FUNC**: A specific coercion function exists and should be called to perform the conversion
- **COERCION_PATH_RELABELTYPE**: The types are binary-compatible, so no actual conversion function is needed (though domain constraints may still apply)
- **COERCION_PATH_ARRAYCOERCE**: Array-to-array coercion is possible using an ArrayCoerceExpr node
- **COERCION_PATH_COERCEVIAIO**: Conversion can be performed using input/output functions (text-based conversion)

## Parameters / Member Variables
- : Indicates failure to find any valid coercion pathway between types
- : Indicates a cast function should be applied; the function OID is returned separately
- : Indicates binary-compatible types that can be relabeled without conversion
- : Indicates array coercion using element-wise conversion within an ArrayCoerceExpr
- : Indicates conversion through serialization/deserialization via I/O functions

## Dependencies
- Functions called/Symbols referenced:
  - Used as return type by find_coercion_pathway
  - Referenced in type coercion logic throughout the parser

- Called from (representative examples):
  - [find_coercion_pathway](../f/find_coercion_pathway.md) (src/backend/parser/parse_coerce.c:3159)
  - [coerce_type](../c/coerce_type.md) (src/backend/parser/parse_coerce.c:162)
  - [can_coerce_type](../c/can_coerce_type.md) (src/backend/parser/parse_coerce.c:567)
  - [build_coercion_expression](../b/build_coercion_expression.md) (src/backend/parser/parse_coerce.c:840)
  - [ATAddForeignKeyConstraint](../A/ATAddForeignKeyConstraint.md) (src/backend/commands/tablecmds.c:9923)
  - [func_get_detail](../f/func_get_detail.md) (src/backend/parser/parse_func.c:1501)

## Notes and Other Information
- The enum values are ordered to facilitate comparison operations in coercion context checking
- COERCION_PATH_RELABELTYPE does not guarantee zero-effort conversion since domain constraints may still need to be applied
- COERCION_PATH_COERCEVIAIO provides a fallback mechanism for types that don't have explicit cast definitions but can be converted through text representation
- The type system uses this enum extensively in expression parsing, function resolution, and constraint checking
- Array coercion (COERCION_PATH_ARRAYCOERCE) recursively applies element-level coercion rules
- Special handling exists for certain built-in array types like oidvector and int2vector to prevent unwanted coercions