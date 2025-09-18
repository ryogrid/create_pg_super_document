# check_valid_polymorphic_signature

## Location
src/backend/parser/parse_coerce.c: 2877 - 2953

## Overview
Validates whether a proposed function signature is valid according to PostgreSQL's polymorphism rules.

## Definition


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
  - psprintf
  - format_type_be
- Called from (representative examples):
  - ProcedureCreate
  - AggregateCreate

## Notes and Other Information
- Located in src/backend/parser/parse_coerce.c:2877-2953
- Returns NULL if the signature is valid, otherwise returns a palloc'd error message string
- Used during CREATE FUNCTION and CREATE AGGREGATE to validate polymorphic signatures
- Error messages are already translated for internationalization
- Critical for preventing invalid polymorphic function definitions at creation time
- The function maintains synchronization with IsPolymorphicTypeFamily1 and IsPolymorphicTypeFamily2 functions