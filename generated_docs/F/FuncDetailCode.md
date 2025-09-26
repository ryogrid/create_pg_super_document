# FuncDetailCode

## Location
[src/include/parser/parse_func.h:31-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_func.h#L31-L74)

## Overview
FuncDetailCode is an enumeration that represents the result codes returned by the PostgreSQL function lookup system, specifically the `func_get_detail` function.

## Definition
```c
typedef enum
{
    FUNCDETAIL_NOTFOUND,        /* no matching function */
    FUNCDETAIL_MULTIPLE,        /* too many matching functions */
    FUNCDETAIL_NORMAL,          /* found a matching regular function */
    FUNCDETAIL_PROCEDURE,       /* found a matching procedure */
    FUNCDETAIL_AGGREGATE,       /* found a matching aggregate function */
    FUNCDETAIL_WINDOWFUNC,      /* found a matching window function */
    FUNCDETAIL_COERCION,        /* it's a type coercion request */
} FuncDetailCode;
```

## Detailed Description
FuncDetailCode is a crucial enumeration used throughout PostgreSQL's function resolution system. When the parser encounters a function call, it uses various lookup mechanisms to determine what kind of function is being invoked. This enum provides detailed information about the outcome of that lookup process, allowing the parser to make appropriate decisions about how to handle the function call.

The enum covers all possible outcomes of function lookup, from complete failure (FUNCDETAIL_NOTFOUND) to successful identification of specific function types like procedures, aggregates, and window functions. It also handles ambiguous cases (FUNCDETAIL_MULTIPLE) and special cases like type coercions.

## Parameters / Member Variables
- `FUNCDETAIL_NOTFOUND`: Indicates that no function matching the given name and argument types could be found in the system catalogs
- `FUNCDETAIL_MULTIPLE`: Indicates that multiple functions match the given criteria, creating an ambiguous situation that requires error handling
- `FUNCDETAIL_NORMAL`: Indicates that a single, regular (non-aggregate, non-window) function was found that matches the criteria
- `FUNCDETAIL_PROCEDURE`: Indicates that the lookup found a stored procedure rather than a regular function
- `FUNCDETAIL_AGGREGATE`: Indicates that the lookup found an aggregate function (like SUM, COUNT, etc.)
- `FUNCDETAIL_WINDOWFUNC`: Indicates that the lookup found a window function (like ROW_NUMBER, RANK, etc.)
- `FUNCDETAIL_COERCION`: Indicates that the function call is actually a type coercion request rather than a true function call

## Dependencies
- Functions called/Symbols referenced:
  - ParseFuncOrColumn
  - func_get_detail
  - FuncCall
  - FuncCandidateList
  - ObjectType
  - ObjectWithArgs

- Called from (representative examples):
  - lookup_agg_function (src/backend/catalog/pg_aggregate.c:837)
  - ParseFuncOrColumn (src/backend/parser/parse_func.c:117)
  - func_select_candidate (src/backend/parser/parse_func.c:1394)
  - func_get_detail (src/backend/parser/parse_func.c:1580)
  - binary_oper_exact (src/backend/parser/parse_oper.c:311)
  - oper (src/backend/parser/parse_oper.c:376)
  - left_oper (src/backend/parser/parse_oper.c:523)

## Notes and Other Information
- This enum is central to PostgreSQL's function resolution system and is used extensively throughout the parser
- The distinction between different function types (regular, procedure, aggregate, window) is important for proper SQL semantics and optimization
- Type coercion detection (FUNCDETAIL_COERCION) is crucial for PostgreSQL's flexible type system
- Error handling for FUNCDETAIL_NOTFOUND and FUNCDETAIL_MULTIPLE cases provides clear diagnostics to users
- The enum values are used in switch statements throughout the codebase to handle different function types appropriately
- Window functions require special parsing and execution logic, hence the separate FUNCDETAIL_WINDOWFUNC category
- Procedures have different calling conventions than functions, necessitating the FUNCDETAIL_PROCEDURE distinction