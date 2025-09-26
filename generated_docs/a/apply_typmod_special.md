# apply_typmod_special

## Location
[src/backend/utils/adt/numeric.c:8008-8044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8008-L8044)

## Overview
The `apply_typmod_special` function handles bounds checking for special numeric values (NaN and Infinity) according to specified type modifier constraints, operating on packed numeric forms.

## Definition

```c
static bool
apply_typmod_special(Numeric num, int32 typmod, Node *escontext)
```
## Detailed Description
This function is a specialized companion to `apply_typmod` that deals exclusively with PostgreSQL's special numeric values:

1. **Input Validation**: Asserts that the input is indeed a special numeric value using `NUMERIC_IS_SPECIAL`
2. **NaN Handling**: Allows NaN values regardless of typmod restrictions (legacy behavior)
3. **Infinity Handling**: Rejects infinite values when any typmod restriction is present, since infinity cannot fit within finite precision constraints
4. **Typmod Validation**: Checks if the typmod is valid before applying restrictions
5. **Error Reporting**: Provides detailed error messages when infinity values violate precision/scale constraints

The function operates on packed numeric representations for efficiency and convenience of most callers.

## Parameters / Member Variables
- `num`: Packed numeric value that must be a special value (NaN or Infinity)
- `typmod`: 32-bit type modifier encoding precision and scale constraints
- `escontext`: Error handling context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - `NUMERIC_IS_SPECIAL`: Macro to check if value is special (NaN/Inf)
  - `NUMERIC_IS_NAN`: Macro to check if value is NaN
  - [is_valid_numeric_typmod](../i/is_valid_numeric_typmod.md): Validates typmod parameter
  - [numeric_typmod_precision](../n/numeric_typmod_precision.md): Extracts precision from typmod
  - [numeric_typmod_scale](../n/numeric_typmod_scale.md): Extracts scale from typmod
  - `ereturn`: Soft error return mechanism
  - `[Numeric](../N/Numeric.md)`: PostgreSQL's packed numeric type

- Called from (representative examples):
  - `NUMERIC_CAN_BE_SHORT`: Short numeric validation path
  - [numeric_in](../n/numeric_in.md): Text input parsing for special values
  - [numeric_recv](../n/numeric_recv.md): Binary input parsing for special values
  - [numeric](../n/numeric.md): Type coercion for special values

## Notes and Other Information
- NaN values are allowed regardless of precision/scale restrictions (historical PostgreSQL behavior)
- Infinity values are always rejected when any typmod constraints are specified
- The function assumes the caller has already verified the input is a special value
- Returns `true` for success, `false` for failure with soft error handling
- Uses packed numeric representation for efficiency
- Error messages provide clear feedback about precision/scale constraints that prevent storing infinite values