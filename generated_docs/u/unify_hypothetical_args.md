# unify_hypothetical_args

## Location
[src/backend/parser/parse_func.c:1741-1824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_func.c#L1741-L1824)

## Overview
Ensures type consistency between hypothetical direct arguments and corresponding aggregated arguments in hypothetical-set aggregates by performing necessary type coercion.

## Definition
```c
static void unify_hypothetical_args(ParseState *pstate,
                                   List *fargs,
                                   int numAggregatedArgs,
                                   Oid *actual_arg_types,
                                   Oid *declared_arg_types)
```

## Detailed Description
unify_hypothetical_args is a specialized function that handles type unification for hypothetical-set aggregates like `rank()` and `percent_rank()`. These aggregates have a unique structure where they take both direct arguments (the hypothetical values) and aggregated arguments (the dataset values), and the corresponding arguments must have compatible types.

The function identifies pairs of hypothetical and aggregated arguments that correspond to each other and ensures they have the same type. When the aggregate is declared with ANY type parameters, the function performs type resolution using PostgreSQL's common type selection logic, similar to UNION operations. It gives preference to the aggregated argument's type to minimize coercion overhead.

The function modifies the argument expressions in place and updates the actual argument type array to reflect any type coercions performed.

## Parameters / Member Variables
- `pstate`: ParseState containing parsing context for error reporting and type coercion
- `fargs`: List of function argument expressions to potentially modify
- `numAggregatedArgs`: Number of aggregated arguments (those appearing after WITHIN GROUP)
- `actual_arg_types`: Array of actual argument type OIDs to be updated
- `declared_arg_types`: Array of declared argument type OIDs from the function definition

## Dependencies
- Functions called/Symbols referenced:
  - [list_nth_cell](../l/list_nth_cell.md) (accesses specific argument positions in the argument list)
  - [select_common_type](../s/select_common_type.md) (determines the best common type for argument pairs)
  - [select_common_typmod](../s/select_common_typmod.md) (determines the best common type modifier)
  - [coerce_type](../c/coerce_type.md) (performs the actual type coercion on arguments)
  - list_make2 (creates temporary lists for type selection)
- Called from (representative examples):
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (from parse_func.c:505)

## Notes and Other Information
- Static function used internally within the parser
- Only operates on hypothetical-set aggregates (AGGKIND_HYPOTHETICAL)
- Performs safety checks to validate aggregate declaration consistency
- Skips processing when declared types are not ANY (letting make_fn_arguments handle coercion)
- Gives preference to aggregated argument types to minimize dataset coercion overhead
- Updates both the argument expressions and the actual_arg_types array
- Critical for proper functioning of statistical aggregates like rank(), dense_rank(), percent_rank(), etc.
- Part of PostgreSQL's advanced aggregate function type system