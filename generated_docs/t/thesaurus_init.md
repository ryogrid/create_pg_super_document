# thesaurus_init

## Location
[src/backend/tsearch/dict_thesaurus.c:596-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/dict_thesaurus.c#L596-L656)

## Overview
Initializes a thesaurus dictionary for text search, setting up the dictionary structure and loading thesaurus data from configuration files.

## Definition

```c
Datum
thesaurus_init(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL function that initializes a thesaurus text search dictionary. It processes initialization parameters from a list of options, validates required parameters, loads thesaurus data from a dictionary file, and sets up the underlying sub-dictionary that will be used for word processing. The function creates a  structure that contains compiled lexeme patterns and substitution rules, along with a reference to the configured sub-dictionary for actual word processing.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : List of DefElem structures containing configuration options (dictfile and dictionary parameters)

## Dependencies
- Functions called/Symbols referenced:
  -  (structure type)
  -  (structure type)  
  -  (loads thesaurus data from file)
  -  (extracts string value from DefElem)
  -  (parses dictionary name)
  -  (gets OID of text search dictionary)
  -  (retrieves cached dictionary)
  -  (compiles lexeme patterns)
  -  (compiles substitution patterns)
- Called from:
  - This is a PostgreSQL dictionary initialization callback function

## Notes and Other Information
- Requires both 'dictfile' and 'dictionary' parameters to be specified
- Validates that parameters are not duplicated
- The dictfile parameter specifies the thesaurus data file to load
- The dictionary parameter specifies the sub-dictionary to use for word processing
- Throws errors for missing required parameters, duplicate parameters, or unrecognized parameters
- Returns a pointer to the initialized DictThesaurus structure

## Simplified Source

```c
Datum thesaurus_init(PG_FUNCTION_ARGS) {
    List *dictoptions = (List *) PG_GETARG_POINTER(0);
    DictThesaurus *d;
    char *subdictname = NULL;
    bool fileloaded = false;
    ListCell *l;

    // Initialize thesaurus dictionary structure
    d = (DictThesaurus *) palloc0(sizeof(DictThesaurus));

    // Process configuration options
    foreach(l, dictoptions) {
        DefElem *defel = (DefElem *) lfirst(l);

        if (strcmp(defel->defname, "dictfile") == 0) {
            if (fileloaded)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple DictFile parameters")));
            // Load thesaurus data from file
            thesaurusRead(defGetString(defel), d);
            fileloaded = true;
        }
        else if (strcmp(defel->defname, "dictionary") == 0) {
            if (subdictname)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                               errmsg("multiple Dictionary parameters")));
            subdictname = pstrdup(defGetString(defel));
        }
        else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("unrecognized Thesaurus parameter: \"%s\"",
                                  defel->defname)));
        }
    }

    // Validate required parameters
    if (!fileloaded)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("missing DictFile parameter")));
    if (!subdictname)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("missing Dictionary parameter")));

    // Setup sub-dictionary reference
    namelist = stringToQualifiedNameList(subdictname, NULL);
    d->subdictOid = get_ts_dict_oid(namelist, false);
    d->subdict = lookup_ts_dictionary_cache(d->subdictOid);

    // Compile thesaurus patterns
    compileTheLexeme(d);
    compileTheSubstitute(d);

    PG_RETURN_POINTER(d);
}
```