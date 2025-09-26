# GetAggInitVal

## Location
[src/backend/executor/nodeAgg.c:4288-4303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4288-L4303)

## Overview
GetAggInitVal converts a text representation of an aggregate's initial value to the appropriate Datum format for the aggregate's transition data type.

## Definition

```c
static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
```
## Detailed Description
This static function is responsible for parsing and converting text-based initial values for aggregates into their proper internal representation. It takes a text datum containing the string representation of an initial value and converts it to a Datum of the specified transition type. The function handles the type conversion by obtaining the appropriate input function for the target type and calling it with the string representation.

## Parameters / Member Variables
- `textInitVal`: A Datum containing the text representation of the initial value to be converted
- `transtype`: The OID of the transition data type that the initial value should be converted to

## Dependencies
- Functions called/Symbols referenced:
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - TextDatumGetCString
  - [OidInputFunctionCall](../O/OidInputFunctionCall.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md) (src/backend/executor/nodeAgg.c:3872)
  - [initialize_peragg](../i/initialize_peragg.md) (src/backend/executor/nodeWindowAgg.c:2959)
  - [preprocess_aggref](../p/preprocess_aggref.md) (src/backend/optimizer/prep/prepagg.c:214)

## Notes and Other Information
- This is a static function within nodeAgg.c, indicating it's used internally for aggregate initialization
- The function properly manages memory by freeing the string representation after conversion
- It's used during both regular aggregate initialization and window aggregate processing
- The conversion process uses PostgreSQL's standard type input mechanisms for type safety