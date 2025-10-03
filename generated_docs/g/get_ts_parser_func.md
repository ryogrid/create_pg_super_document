# get_ts_parser_func

## Location
[src/backend/commands/tsearchcmds.c:74-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tsearchcmds.c#L74-L136)

## Overview
This function looks up a text search parser support function and returns its OID as a Datum, validating that the function signature matches the expected interface for the specified parser function type.

## Definition

```c
static Datum
get_ts_parser_func(DefElem *defel, int attnum)
```
## Detailed Description
The function serves as a validation and lookup mechanism for text search parser functions in PostgreSQL. It takes a parser function definition element and determines the expected function signature based on the attribute number (attnum), which corresponds to specific columns in the pg_ts_parser system catalog. The function then looks up the specified function name and validates that its signature matches the expected interface for that type of parser function.

The function handles five different types of parser functions:
- prsstart: Parser initialization function (2 args: internal, int4 -> internal)
- prstoken: Token parsing function (3 args: internal, internal, internal -> internal) 
- prsend: Parser cleanup function (1 arg: internal -> void)
- prsheadline: Headline generation function (3 args: internal, internal, tsquery -> internal)
- prslextype: Lexical type enumeration function (1 arg: internal -> internal)

## Parameters / Member Variables
- `*defel`: DefElem pointer containing the qualified function name from the parser definition
- `attnum`: Integer indicating which pg_ts_parser column/function type this represents (Anum_pg_ts_parser_*)
## Dependencies
- Functions called/Symbols referenced:
  - [defGetQualifiedName](../d/defGetQualifiedName.md): Extracts qualified function name from DefElem
  - [LookupFuncName](../L/LookupFuncName.md): Looks up function OID by name and signature
  - [get_func_rettype](get_func_rettype.md): Gets the return type of a function
  - [func_signature_string](../f/func_signature_string.md): Formats function signature for error messages
  - [format_type_be](../f/format_type_be.md): Formats type name for error messages
- Called from (representative examples):
  - [DefineTSParser](../D/DefineTSParser.md): Used to validate and retrieve OIDs for all parser function types

## Notes and Other Information
- This is a static function, only accessible within tsearchcmds.c
- The function performs strict signature validation to ensure type safety for parser functions
- The prslextype function requires an internal-type argument for security reasons, even though the argument is not actually used
- Error reporting includes detailed function signature information to help diagnose type mismatches
- All parser functions except prsend return type 'internal'; prsend returns 'void'

## Simplified Source

```c
static Datum get_ts_parser_func(DefElem *defel, int attnum) {
    List *funcName = defGetQualifiedName(defel);
    Oid typeId[3];
    Oid retTypeId;
    int nargs;
    Oid procOid;

    // Set default return type and first parameter
    retTypeId = INTERNALOID;
    typeId[0] = INTERNALOID;

    // Determine function signature based on parser function type
    switch (attnum) {
        case Anum_pg_ts_parser_prsstart:
            nargs = 2;
            typeId[1] = INT4OID;
            break;
        case Anum_pg_ts_parser_prstoken:
            nargs = 3;
            typeId[1] = INTERNALOID;
            typeId[2] = INTERNALOID;
            break;
        case Anum_pg_ts_parser_prsend:
            nargs = 1;
            retTypeId = VOIDOID;
            break;
        case Anum_pg_ts_parser_prsheadline:
            nargs = 3;
            typeId[1] = INTERNALOID;
            typeId[2] = TSQUERYOID;
            break;
        case Anum_pg_ts_parser_prslextype:
            nargs = 1;
            break;
        default:
            elog(ERROR, "unrecognized attribute for text search parser: %d", attnum);
    }

    // Look up function and validate return type
    procOid = LookupFuncName(funcName, nargs, typeId, false);
    if (get_func_rettype(procOid) != retTypeId)
        ereport(ERROR, "function should return correct type");

    return ObjectIdGetDatum(procOid);
}
```