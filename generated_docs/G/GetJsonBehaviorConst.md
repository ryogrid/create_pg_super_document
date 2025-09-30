# GetJsonBehaviorConst

## Location
[src/backend/parser/parse_expr.c:4835-4893](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4835-L4893)

## Overview
Returns a Const node holding the appropriate constant value for a given non-ERROR JsonBehaviorType during JSON function parsing.

## Definition
```c
static Node *
GetJsonBehaviorConst(JsonBehaviorType btype, int location)
```

## Detailed Description
This function creates constant expression nodes for various JSON behavior types used in SQL/JSON functions. It maps JsonBehaviorType enum values to their corresponding constant representations: empty arrays/objects are created as jsonb constants, boolean behaviors become boolean constants, and NULL-like behaviors become NULL integer constants. The function handles all behavior types except JSON_BEHAVIOR_DEFAULT and JSON_BEHAVIOR_ERROR, which are processed by the calling code.

The function creates properly typed Const nodes with correct type information, including setting the location for error reporting purposes. Each behavior type results in a different constant value and data type.

## Parameters / Member Variables
- `btype`: JsonBehaviorType specifying which behavior constant to create
- `location`: Source location for error reporting and debugging

## Dependencies
- Functions called/Symbols referenced:
  - DirectFunctionCall1
  - [jsonb_in](../j/jsonb_in.md)
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [makeConst](../m/makeConst.md)
  - JSON_BEHAVIOR_EMPTY_ARRAY
  - JSON_BEHAVIOR_EMPTY_OBJECT
  - JSON_BEHAVIOR_TRUE
  - JSON_BEHAVIOR_FALSE
  - JSON_BEHAVIOR_NULL
  - JSON_BEHAVIOR_UNKNOWN
  - JSON_BEHAVIOR_EMPTY
  - JSON_BEHAVIOR_DEFAULT
  - JSON_BEHAVIOR_ERROR
- Called from (representative examples):
  - [transformJsonBehavior](../t/transformJsonBehavior.md)

## Notes and Other Information
The function uses different constant types depending on the behavior: JSONBOID for empty arrays/objects, BOOLOID for true/false behaviors, and INT4OID for NULL-like behaviors. JSON_BEHAVIOR_DEFAULT and JSON_BEHAVIOR_ERROR cases contain assertions that should never be reached, as these are handled by the caller. The function ensures proper memory representation by setting isbyval and length appropriately for each data type.

## Simplified Source

```c
static Node *
GetJsonBehaviorConst(JsonBehaviorType btype, int location)
{
    Datum val = (Datum) 0;
    Oid typid = JSONBOID;
    int len = -1;
    bool isbyval = false;
    bool isnull = false;
    Const *con;

    switch (btype) {
        case JSON_BEHAVIOR_EMPTY_ARRAY:
            val = DirectFunctionCall1(jsonb_in, CStringGetDatum("[]"));
            break;

        case JSON_BEHAVIOR_EMPTY_OBJECT:
            val = DirectFunctionCall1(jsonb_in, CStringGetDatum("{}"));
            break;

        case JSON_BEHAVIOR_TRUE:
            val = BoolGetDatum(true);
            typid = BOOLOID;
            len = sizeof(bool);
            isbyval = true;
            break;

        case JSON_BEHAVIOR_FALSE:
            val = BoolGetDatum(false);
            typid = BOOLOID;
            len = sizeof(bool);
            isbyval = true;
            break;

        case JSON_BEHAVIOR_NULL:
        case JSON_BEHAVIOR_UNKNOWN:
        case JSON_BEHAVIOR_EMPTY:
            // NULL values with integer type
            val = (Datum) 0;
            isnull = true;
            typid = INT4OID;
            len = sizeof(int32);
            isbyval = true;
            break;

        case JSON_BEHAVIOR_DEFAULT:
        case JSON_BEHAVIOR_ERROR:
            Assert(false); // Handled by caller
            break;

        default:
            elog(ERROR, "unrecognized SQL/JSON behavior %d", btype);
            break;
    }

    con = makeConst(typid, -1, InvalidOid, len, val, isnull, isbyval);
    con->location = location;

    return (Node *) con;
}
```