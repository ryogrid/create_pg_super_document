# typenameTypeMod

## Location
[src/backend/parser/parse_type.c:332-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L332-L438)

## Overview
A static function that processes type modifier expressions from a TypeName structure and converts them into the internal typmod integer value used by PostgreSQL's type system.

## Definition

```c
struct_array_builtin(datums, n, CSTRINGOID);
```
## Detailed Description
This function handles the complex process of converting user-specified type modifiers (like precision and scale in NUMERIC(10,2)) into the internal typmod format. It validates that the target type supports type modifiers, processes the list of modifier expressions, and calls the type's typmodin function to generate the final typmod value.

The function supports various forms of modifier expressions including numeric constants, string literals, and simple identifiers. It performs thorough error checking to ensure type modifiers are only applied to types that support them and that the modifier expressions are in valid format.

The process involves converting raw grammar expressions to an array of cstrings, then passing this array to the type's typmodin function which encodes the modifiers into a single int32 value.

## Parameters / Member Variables
- : Parse state context for error reporting and location tracking (may be NULL)
- : TypeName structure containing the type specification and modifier expressions
- : The already-resolved Type tuple from the system catalog

## Dependencies
- Functions called/Symbols referenced:
  - [TypeNameToString](../T/TypeNameToString.md)
  - intVal
  - strVal
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [construct_array_builtin](../c/construct_array_builtin.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - OidFunctionCall1
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)
- Called from (representative examples):
  - [LookupTypeNameExtended](../L/LookupTypeNameExtended.md)

## Notes and Other Information
This is a static function internal to parse_type.c, designed to be called only from within the type resolution process. It performs careful validation of shell types (incomplete type definitions) and provides specific error messages for various failure cases. The function handles memory management for temporary arrays and ensures proper cleanup through pfree calls. Type modifiers are a PostgreSQL extension that allows types like VARCHAR(50) or NUMERIC(10,2) to specify additional constraints or parameters.

## Simplified Source

```c
static int32 typenameTypeMod(ParseState *pstate, const TypeName *typeName, Type typ) {
    int32 result;
    Oid typmodin;
    Datum *datums;
    int n;

    // Return prespecified typmod if no expressions to process
    if (typeName->typmods == NIL) {
        return typeName->typemod;
    }

    // Check if type supports modifiers (not a shell type)
    if (!((Form_pg_type) GETSTRUCT(typ))->typisdefined) {
        ereport(ERROR, "type modifier cannot be specified for shell type");
    }

    // Get the type's typmodin function
    typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;
    if (typmodin == InvalidOid) {
        ereport(ERROR, "type modifier is not allowed for this type");
    }

    // Convert modifier expressions to string array
    datums = (Datum *) palloc(list_length(typeName->typmods) * sizeof(Datum));
    n = 0;

    foreach(l, typeName->typmods) {
        Node *tm = (Node *) lfirst(l);
        char *cstr = NULL;

        if (IsA(tm, A_Const)) {
            A_Const *ac = (A_Const *) tm;

            if (IsA(&ac->val, Integer)) {
                cstr = psprintf("%ld", (long) intVal(&ac->val));
            } else if (IsA(&ac->val, Float)) {
                cstr = ac->val.fval.fval;  // Use string representation directly
            } else if (IsA(&ac->val, String)) {
                cstr = strVal(&ac->val);   // Use string value directly
            }
        } else if (IsA(tm, ColumnRef)) {
            ColumnRef *cr = (ColumnRef *) tm;

            // Handle simple identifiers
            if (list_length(cr->fields) == 1 &&
                IsA(linitial(cr->fields), String)) {
                cstr = strVal(linitial(cr->fields));
            }
        }

        if (!cstr) {
            ereport(ERROR, "type modifiers must be simple constants or identifiers");
        }

        datums[n++] = CStringGetDatum(cstr);
    }

    // Create array of modifier strings
    ArrayType *arrtypmod = construct_array_builtin(datums, n, CSTRINGOID);

    // Call type's typmodin function to compute final typmod
    ParseCallbackState pcbstate;
    setup_parser_errposition_callback(&pcbstate, pstate, typeName->location);

    result = DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)));

    cancel_parser_errposition_callback(&pcbstate);

    // Clean up allocated memory
    pfree(datums);
    pfree(arrtypmod);

    return result;
}
```