# IsPreferredType

## Location
[src/backend/parser/parse_coerce.c:2997-3031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L2997-L3031)

## Overview
Checks if a given type is a preferred type within its category or within a specified category.

## Definition

```c
bool
IsPreferredType(TYPCATEGORY category, Oid type)
```
## Detailed Description
This function determines whether a specified type is marked as "preferred" within its type category. Preferred types are used by PostgreSQL's type resolution system to break ties when multiple types could potentially be used for an operation.

In PostgreSQL's type system, each category can have one or more preferred types. When the parser needs to resolve ambiguous cases (such as choosing between multiple possible interpretations of a literal constant or selecting among overloaded functions), it gives preference to these designated preferred types.

For example, in the numeric category,  and  are typically preferred types, so an untyped literal like  will be resolved to  by default, and  will be resolved to .

The function can operate in two modes:
1. **Category-specific check**: When a specific category is provided, it checks if the type is preferred within that exact category
2. **General preferred check**: When TYPCATEGORY_INVALID is passed as the category, it returns true if the type is preferred in any category

## Parameters / Member Variables
- `category`: The type category to check against (or TYPCATEGORY_INVALID to check if preferred in any category)
- `type`: The OID of the type to check
## Dependencies
- Functions called/Symbols referenced:
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - TYPCATEGORY (type definition)
  - TYPCATEGORY_INVALID
- Called from (representative examples):
  - [GetDefaultOpClass](../G/GetDefaultOpClass.md)
  - [func_select_candidate](../f/func_select_candidate.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2997-3031
- Returns true if the type is preferred within the specified category (or any category if TYPCATEGORY_INVALID is used)
- Critical component of PostgreSQL's type resolution and function overloading system
- Used during operator and function resolution to break ties between equally valid candidates
- Preferred type information is stored in the pg_type system catalog
- Examples of preferred types include:
  - int4 and float8 in numeric category
  - [text](../t/text.md) in string category
  - timestamptz in datetime category
- The preferred type mechanism helps ensure predictable behavior in ambiguous type resolution scenarios

## Simplified Source

```c
bool
IsPreferredType(TYPCATEGORY category, Oid type)
{
    char typcategory;
    bool typispreferred;

    // Get type's category and preferred flag
    get_type_category_preferred(type, &typcategory, &typispreferred);

    // Check if type is preferred in the specified category
    if (category == typcategory || category == TYPCATEGORY_INVALID)
        return typispreferred;
    else
        return false;
}
```