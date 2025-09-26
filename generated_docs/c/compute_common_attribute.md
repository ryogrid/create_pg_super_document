# compute_common_attribute

## Location
[src/backend/commands/functioncmds.c:500-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/functioncmds.c#L500-L601)

## Overview
Recognizes and processes common function attributes that can be specified in both CREATE FUNCTION and ALTER FUNCTION statements, while enforcing procedure-specific restrictions and preventing duplicate clauses.

## Definition

```c
static bool
compute_common_attribute(ParseState *pstate,
						 bool is_procedure,
						 DefElem *defel,
						 DefElem **volatility_item,
						 DefElem **strict_item,
						 DefElem **security_item,
						 DefElem **leakproof_item,
						 List **set_items,
						 DefElem **cost_item,
						 DefElem **rows_item,
						 DefElem **support_item,
						 DefElem **parallel_item)
```
## Detailed Description
This function parses individual function attribute definition elements and categorizes them into the appropriate output parameters. It enforces important restrictions by preventing procedures from using function-specific attributes like volatility, strict, leakproof, cost, rows, support, and parallel. The function also prevents duplicate attribute specifications by checking if an output parameter already points to a non-NULL value and reporting conflicts when detected. The 'set' attribute is handled specially by allowing multiple values to be accumulated in a list rather than being treated as a duplicate.

## Parameters / Member Variables
- : ParseState for error reporting with location information
- : Boolean flag indicating whether this is a procedure (not a function)
- : DefElem containing the attribute definition to process
- : Output pointer for VOLATILE/STABLE/IMMUTABLE attribute
- : Output pointer for STRICT/CALLED ON NULL INPUT attribute
- : Output pointer for SECURITY DEFINER/INVOKER attribute
- : Output pointer for LEAKPROOF attribute
- : Output list for SET configuration parameter settings
- : Output pointer for COST attribute
- : Output pointer for ROWS attribute
- : Output pointer for SUPPORT function attribute  
- : Output pointer for PARALLEL SAFE/RESTRICTED/UNSAFE attribute

## Dependencies
- Functions called/Symbols referenced:
  - [errorConflictingDefElem](../e/errorConflictingDefElem.md): Reports duplicate attribute errors with location
  - [lappend](../l/lappend.md): Appends SET configuration items to list
  - [parser_errposition](../p/parser_errposition.md): Provides error position information
- Called from (representative examples):
  - [compute_function_attributes](compute_function_attributes.md): Function attribute processing during creation
  - [AlterFunction](../A/AlterFunction.md): Function attribute modification during alteration

## Notes and Other Information
- Returns true if the attribute was recognized and processed, false otherwise
- Procedures cannot use function-specific optimization attributes (volatility, strict, leakproof, cost, rows, support, parallel)
- The 'security' attribute is the only attribute besides 'set' that procedures can use
- SET configuration parameters can be specified multiple times and are accumulated in a list
- All other attributes are checked for duplicates and raise errors if specified more than once
- Error messages include precise location information for better user experience
- The function uses a goto label 'procedure_error' for consistent error handling of invalid procedure attributes