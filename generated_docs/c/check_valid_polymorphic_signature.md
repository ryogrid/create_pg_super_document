# check_valid_polymorphic_signature

## Location
[src/backend/parser/parse_coerce.c:2877-2953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L2877-L2953)

## Overview
Validates whether a proposed function signature is valid according to PostgreSQL's polymorphism rules.

## Definition

```c
enum, anyrange, or anymultirange."),
						format_type_be(ret_type));
```
## Detailed Description
This function validates function signatures that involve polymorphic types by checking that the return type can be properly deduced from the input arguments. It ensures that functions with polymorphic return types have sufficient polymorphic input arguments to determine the concrete type.

The function applies different validation rules based on the polymorphic return type:

**ANYRANGE/ANYMULTIRANGE return types**: Require at least one ANYRANGE or ANYMULTIRANGE input argument, since multiple range types can have the same element type and the function needs to know which specific range type to return.

**ANYCOMPATIBLERANGE/ANYCOMPATIBLEMULTIRANGE return types**: Require at least one ANYCOMPATIBLERANGE or ANYCOMPATIBLEMULTIRANGE input argument for the same reason as above.

**Other Family-1 polymorphic return types**: Require at least one Family-1 polymorphic input argument (ANYELEMENT, ANYARRAY, ANYNONARRAY, ANYENUM, ANYRANGE, or ANYMULTIRANGE).

**Other Family-2 polymorphic return types**: Require at least one Family-2 polymorphic input argument (ANYCOMPATIBLE, ANYCOMPATIBLEARRAY, ANYCOMPATIBLENONARRAY, ANYCOMPATIBLERANGE, or ANYCOMPATIBLEMULTIRANGE).

## Parameters / Member Variables
- : The return type OID of the function being validated
- : Array of declared argument type OIDs for the function
- : Number of function arguments

## Dependencies
- Functions called/Symbols referenced:
  - IsPolymorphicTypeFamily1
  - IsPolymorphicTypeFamily2
  - [psprintf](../p/psprintf.md)
  - [format_type_be](../f/format_type_be.md)
- Called from (representative examples):
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [AggregateCreate](../A/AggregateCreate.md)

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2877-2953
- Returns NULL if the signature is valid, otherwise returns a palloc'd error message string
- Used during CREATE FUNCTION and CREATE AGGREGATE to validate polymorphic signatures
- Error messages are already translated for internationalization
- Critical for preventing invalid polymorphic function definitions at creation time
- The function maintains synchronization with IsPolymorphicTypeFamily1 and IsPolymorphicTypeFamily2 functions

## Simplified Source

```c
char *
check_valid_polymorphic_signature(Oid ret_type, const Oid *declared_arg_types, int nargs)
{
    // Check range types - need compatible range input
    if (ret_type == ANYRANGEOID || ret_type == ANYMULTIRANGEOID) {
        for (int i = 0; i < nargs; i++) {
            if (declared_arg_types[i] == ANYRANGEOID || declared_arg_types[i] == ANYMULTIRANGEOID)
                return NULL;  // Valid
        }
        return psprintf(_("A result of type %s requires at least one input of type anyrange or anymultirange."),
                       format_type_be(ret_type));
    }

    // Check compatible range types - need compatible range input
    if (ret_type == ANYCOMPATIBLERANGEOID || ret_type == ANYCOMPATIBLEMULTIRANGEOID) {
        for (int i = 0; i < nargs; i++) {
            if (declared_arg_types[i] == ANYCOMPATIBLERANGEOID || declared_arg_types[i] == ANYCOMPATIBLEMULTIRANGEOID)
                return NULL;  // Valid
        }
        return psprintf(_("A result of type %s requires at least one input of type anycompatiblerange or anycompatiblemultirange."),
                       format_type_be(ret_type));
    }

    // Check Family-1 polymorphic types
    if (IsPolymorphicTypeFamily1(ret_type)) {
        for (int i = 0; i < nargs; i++) {
            if (IsPolymorphicTypeFamily1(declared_arg_types[i]))
                return NULL;  // Valid
        }
        return psprintf(_("A result of type %s requires at least one input of type anyelement, anyarray, anynonarray, anyenum, anyrange, or anymultirange."),
                       format_type_be(ret_type));
    }

    // Check Family-2 polymorphic types
    if (IsPolymorphicTypeFamily2(ret_type)) {
        for (int i = 0; i < nargs; i++) {
            if (IsPolymorphicTypeFamily2(declared_arg_types[i]))
                return NULL;  // Valid
        }
        return psprintf(_("A result of type %s requires at least one input of type anycompatible, anycompatiblearray, anycompatiblenonarray, anycompatiblerange, or anycompatiblemultirange."),
                       format_type_be(ret_type));
    }

    return NULL;  // Non-polymorphic return type is always valid
}
```