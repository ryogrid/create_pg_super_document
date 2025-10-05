# dispell_init

## Location
[src/backend/tsearch/dict_ispell.c:30-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_ispell.c#L30-L110)

## Overview
Initializes an Ispell dictionary object for text search by parsing configuration parameters and loading dictionary, affix, and stopword files.

## Definition
```c
Datum dispell_init(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dispell_init` function is the initialization routine for PostgreSQL's Ispell text search dictionary. It processes a list of configuration parameters to set up a `DictISpell` structure that contains the necessary data for performing morphological analysis and word normalization. The function handles three types of configuration files:

1. **Dictionary file** (`dictfile`): Contains the base word forms and their variations
2. **Affix file** (`afffile`): Contains prefix and suffix rules for word transformation
3. **Stopwords file** (`stopwords`): Contains words to be filtered out during lexical analysis

The function ensures that both dictionary and affix files are provided (as they are mandatory), while stopwords are optional. After loading the files, it sorts the dictionary and affix data structures to optimize lookup performance.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to:
  - `dictoptions`: A `List` pointer containing `DefElem` structures with configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md): Allocates zero-initialized memory
  - [NIStartBuild](../N/NIStartBuild.md): Initializes the NIspell object structure
  - [NISortDictionary](../N/NISortDictionary.md): Sorts dictionary entries for optimal lookup
  - [NISortAffixes](../N/NISortAffixes.md): Sorts affix rules for optimal lookup  
  - [NIFinishBuild](../N/NIFinishBuild.md): Finalizes the NIspell object after loading
  - [NIImportDictionary](../N/NIImportDictionary.md): Loads dictionary data from file
  - `[NIImportAffixes](../N/NIImportAffixes.md)`: Loads affix rules from file
  - [get_tsearch_config_filename](../g/get_tsearch_config_filename.md): Resolves configuration file paths
  - [defGetString](defGetString.md): Extracts string values from DefElem structures
  - [readstoplist](../r/readstoplist.md): Loads stopword list from file
  - [lowerstr](../l/lowerstr.md): Function for lowercase string processing
  - `DictISpell`: Main dictionary structure type
  - [DefElem](../D/DefElem.md): Configuration element structure
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL function call mechanism)

## Notes and Other Information
- This function is designed to be called through PostgreSQL's function call interface as part of text search dictionary initialization
- The function performs comprehensive error checking, ensuring no duplicate parameters are provided
- Both `dictfile` and `afffile` parameters are mandatory; the function will error if either is missing
- The `stopwords` parameter is optional and can be omitted
- File paths are resolved using PostgreSQL's text search configuration directory structure
- Memory allocation uses PostgreSQL's memory management system (`palloc0`)
- The function returns a `Datum` containing a pointer to the initialized `DictISpell` structure

## Simplified Source

```c
Datum dispell_init(PG_FUNCTION_ARGS)
{
    List *dictoptions = (List *) PG_GETARG_POINTER(0);
    DictISpell *d;
    bool affloaded = false, dictloaded = false, stoploaded = false;
    ListCell *l;

    // Allocate and initialize dictionary structure
    d = (DictISpell *) palloc0(sizeof(DictISpell));
    NIStartBuild(&(d->obj));

    // Process configuration parameters
    foreach(l, dictoptions)
    {
        DefElem *defel = (DefElem *) lfirst(l);

        if (strcmp(defel->defname, "dictfile") == 0)
        {
            if (dictloaded)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple DictFile parameters")));
            NIImportDictionary(&(d->obj),
                              get_tsearch_config_filename(defGetString(defel), "dict"));
            dictloaded = true;
        }
        else if (strcmp(defel->defname, "afffile") == 0)
        {
            if (affloaded)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple AffFile parameters")));
            NIImportAffixes(&(d->obj),
                           get_tsearch_config_filename(defGetString(defel), "affix"));
            affloaded = true;
        }
        else if (strcmp(defel->defname, "stopwords") == 0)
        {
            if (stoploaded)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple StopWords parameters")));
            readstoplist(defGetString(defel), &(d->stoplist), lowerstr);
            stoploaded = true;
        }
        else
        {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("unrecognized Ispell parameter: \"%s\"", defel->defname)));
        }
    }

    // Validate required parameters and finalize
    if (affloaded && dictloaded)
    {
        NISortDictionary(&(d->obj));
        NISortAffixes(&(d->obj));
    }
    else if (!affloaded)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("missing AffFile parameter")));
    }
    else
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("missing DictFile parameter")));
    }

    NIFinishBuild(&(d->obj));

    PG_RETURN_POINTER(d);
}
```