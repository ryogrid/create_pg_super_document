# dsnowball_init

## Location
[src/backend/snowball/dict_snowball.c:220-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/dict_snowball.c#L220-L269)

## Overview
This function initializes a Snowball dictionary instance by parsing configuration options and setting up the stemmer module and optional stopword list.

## Definition
```c
Datum dsnowball_init(PG_FUNCTION_ARGS)
```

## Detailed Description
The function serves as the initialization entry point for Snowball text search dictionaries in PostgreSQL. It processes a list of configuration options provided during dictionary creation, specifically handling 'language' and 'stopwords' parameters. The function validates that required parameters are present and that no parameters are duplicated. It allocates and configures a DictSnowball structure that will be used for subsequent text processing operations.

## Parameters / Member Variables
- Function receives PG_FUNCTION_ARGS which contains:
  - `dictoptions`: List of DefElem structures containing configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [locate_stem_module](../l/locate_stem_module.md)
  - [readstoplist](../r/readstoplist.md)
  - [defGetString](defGetString.md)
  - [lowerstr](../l/lowerstr.md)
  - ereport
  - CurrentMemoryContext
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL text search framework (referenced by MININT)

## Notes and Other Information
- This is a PostgreSQL function that follows the standard PG function calling convention
- The function enforces that exactly one 'language' parameter must be provided
- Multiple 'stopwords' or 'language' parameters result in errors
- The function returns a pointer to the initialized DictSnowball structure
- Memory allocation is done in the current memory context for proper cleanup
- Unrecognized parameters trigger configuration errors with descriptive messages

## Simplified Source

```c
Datum dsnowball_init(PG_FUNCTION_ARGS) {
    List *dictoptions = (List *) PG_GETARG_POINTER(0);
    DictSnowball *d;
    bool stoploaded = false;
    ListCell *l;

    // Allocate dictionary structure
    d = (DictSnowball *) palloc0(sizeof(DictSnowball));

    // Process configuration options
    foreach(l, dictoptions) {
        DefElem *defel = (DefElem *) lfirst(l);

        if (strcmp(defel->defname, "stopwords") == 0) {
            // Handle stopwords parameter
            if (stoploaded)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple StopWords parameters")));
            readstoplist(defGetString(defel), &d->stoplist, lowerstr);
            stoploaded = true;
        }
        else if (strcmp(defel->defname, "language") == 0) {
            // Handle language parameter
            if (d->stem)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple Language parameters")));
            locate_stem_module(d, defGetString(defel));
        }
        else {
            // Unknown parameter
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("unrecognized Snowball parameter: \"%s\"",
                                  defel->defname)));
        }
    }

    // Validate required language parameter
    if (!d->stem)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("missing Language parameter")));

    d->dictCtx = CurrentMemoryContext;

    PG_RETURN_POINTER(d);
}
```