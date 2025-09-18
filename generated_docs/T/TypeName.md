# TypeName

## Location
src/include/nodes/parsenodes.h: 265 - 276

## Overview
TypeName is a parse tree node that specifies type information in SQL statements, supporting both named types and OID-based type identification with comprehensive type modifier and array bound specifications.

## Definition
```c
typedef struct TypeName
{
    NodeTag     type;
    List       *names;          /* qualified name (list of String nodes) */
    Oid         typeOid;        /* type identified by OID */
    bool        setof;          /* is a set? */
    bool        pct_type;       /* %TYPE specified? */
    List       *typmods;        /* type modifier expression(s) */
    int32       typemod;        /* prespecified type modifier */
    List       *arrayBounds;    /* array bounds */
    ParseLoc    location;       /* token location, or -1 if unknown */
} TypeName;
```

## Detailed Description
TypeName serves as PostgreSQL's universal representation for type specifications in SQL statements. It supports multiple modes of type identification: by qualified name (schema.type), by internal OID, and by field reference (%TYPE). The structure is designed to handle both simple and complex type specifications including type modifiers, array dimensions, and set types.

For internally generated TypeName structures, it's often more efficient to specify types by OID rather than name. When names is NIL, typeOid contains the actual type OID; otherwise typeOid is unused. Similarly, when typmods is NIL, typemod contains the prespecified type modifier; otherwise typemod is unused.

The pct_type flag enables PostgreSQL's %TYPE functionality, where instead of specifying a type directly, you reference the type of an existing table column or variable.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TypeName node
- `names`: List of String nodes representing qualified type name (e.g., ["pg_catalog", "int4"])
- `typeOid`: Type OID when type is specified by OID rather than name
- `setof`: Boolean flag indicating if this is a SETOF type (for functions returning sets)
- `pct_type`: Boolean flag indicating %TYPE specification (reference to existing field type)
- `typmods`: List of expressions specifying type modifiers (e.g., length, precision)
- `typemod`: Prespecified type modifier value when typmods is not used
- `arrayBounds`: List specifying array dimensions and bounds
- `location`: Source location of the type specification in the original SQL text

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc
- Called from (representative examples):
  - [LookupTypeName](../L/LookupTypeName.md)
  - [typenameType](../t/typenameType.md)
  - [typenameTypeId](../t/typenameTypeId.md)
  - makeTypeName
  - [TypeNameToString](TypeNameToString.md)
  - [parseTypeString](../p/parseTypeString.md)
  - [CreateFunction](../C/CreateFunction.md)
  - [DefineOperator](../D/DefineOperator.md)
  - [AlterEnum](../A/AlterEnum.md)

## Notes and Other Information
- [TypeName](TypeName.md) is used extensively throughout the parser and analyzer for all type-related operations
- Supports both compile-time (OID-based) and runtime (name-based) type resolution
- The %TYPE feature allows for type inheritance from existing schema objects
- Array bounds can specify multi-dimensional arrays with varying dimension specifications  
- Type modifiers support complex type parameterization (precision, scale, length, etc.)
- Used in DDL statements (CREATE TABLE, ALTER TABLE, etc.) and function definitions
- The structure supports PostgreSQL's rich type system including domains, composite types, and arrays
- Location tracking enables accurate error reporting and IDE integration