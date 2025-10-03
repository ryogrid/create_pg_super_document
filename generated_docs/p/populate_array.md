# populate_array

## Location
[src/backend/utils/adt/jsonfuncs.c:2913-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2913-L2979)

## Overview
The main entry point function that converts JSON or JSONB values into PostgreSQL multi-dimensional array structures.

## Definition

```c
static Datum
populate_array(ArrayIOData *aio,
			   const char *colname,
			   MemoryContext mcxt,
			   JsValue *jsv,
			   bool *isnull,
			   Node *escontext)
```
## Detailed Description
This function serves as the primary interface for converting JSON or JSONB data into PostgreSQL arrays. It initializes a PopulateArrayContext with the necessary array metadata and memory contexts, then dispatches to the appropriate parsing function based on whether the input is JSON text or binary JSONB. The function handles both text JSON (via populate_array_json) and binary JSONB (via populate_array_dim_jsonb) formats. After successful parsing, it constructs the final multi-dimensional array using PostgreSQL's array building infrastructure, setting appropriate lower bounds and managing memory cleanup.

## Parameters / Member Variables
- `*aio`: ArrayIOData pointer containing array type information and I/O functions
- `*colname`: Character pointer to the column name for error reporting purposes
- `mcxt`: MemoryContext for array element allocations during parsing
- `*jsv`: JsValue pointer containing either JSON text or JSONB binary data to convert
- `*isnull`: Boolean pointer set to true if parsing errors occur, false on success
- `*escontext`: Node pointer for error context and soft error handling
## Dependencies
- Functions called/Symbols referenced:
  - [ArrayIOData](../A/ArrayIOData.md) (array metadata structure)
  - [JsValue](../J/JsValue.md) (JSON/JSONB value wrapper)
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (parsing context structure)
  - [initArrayResult](../i/initArrayResult.md) (array building state initialization)
  - [populate_array_json](populate_array_json.md) (JSON text parsing)
  - [populate_array_dim_jsonb](populate_array_dim_jsonb.md) (JSONB binary parsing)
  - [makeMdArrayResult](../m/makeMdArrayResult.md) (multi-dimensional array construction)
- Called from (representative examples):
  - JsObjectFree
  - [populate_record_field](populate_record_field.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, serving as an internal implementation detail
- Returns a Datum representing the constructed PostgreSQL array, or (Datum) 0 on error
- The function handles memory management by using separate contexts for different allocation lifetimes
- Lower bounds for all dimensions are set to 1, following PostgreSQL array conventions
- Supports error-safe operation through the escontext parameter, allowing callers to handle errors gracefully
- The function assumes that dimension information will be determined during the parsing process
- Memory cleanup is performed for temporary allocations (dims, sizes, lbs arrays) but not for the final array result
- Part of PostgreSQL's JSON-to-native-type conversion infrastructure, enabling seamless integration between JSON data and PostgreSQL's type system

## Simplified Source

```c
static Datum populate_array(ArrayIOData *aio,
                           const char *colname,
                           MemoryContext mcxt,
                           JsValue *jsv,
                           bool *isnull,
                           Node *escontext) {
    PopulateArrayContext ctx;
    Datum result;
    int *lbs;
    int i;

    // Initialize parsing context
    ctx.aio = aio;
    ctx.mcxt = mcxt;
    ctx.acxt = CurrentMemoryContext;
    ctx.astate = initArrayResult(aio->element_type, ctx.acxt, true);
    ctx.colname = colname;
    ctx.ndims = 0;      // To be determined during parsing
    ctx.dims = NULL;
    ctx.sizes = NULL;
    ctx.escontext = escontext;

    // Parse based on JSON format
    if (jsv->is_json) {
        // Parse JSON text format
        if (!populate_array_json(&ctx, jsv->val.json.str,
                                jsv->val.json.len >= 0 ? jsv->val.json.len
                                                      : strlen(jsv->val.json.str))) {
            *isnull = true;
            return (Datum) 0;
        }
    } else {
        // Parse JSONB binary format
        if (!populate_array_dim_jsonb(&ctx, jsv->val.jsonb, 1)) {
            *isnull = true;
            return (Datum) 0;
        }
        ctx.dims[0] = ctx.sizes[0];
    }

    // Build final array with discovered dimensions
    Assert(ctx.ndims > 0);

    // Set lower bounds to 1 for all dimensions
    lbs = palloc(sizeof(int) * ctx.ndims);
    for (i = 0; i < ctx.ndims; i++) {
        lbs[i] = 1;
    }

    // Create multi-dimensional array
    result = makeMdArrayResult(ctx.astate, ctx.ndims, ctx.dims, lbs,
                              ctx.acxt, true);

    // Cleanup temporary allocations
    pfree(ctx.dims);
    pfree(ctx.sizes);
    pfree(lbs);

    *isnull = false;
    return result;
}
```