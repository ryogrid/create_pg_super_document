# CheckAffix

## Location
[src/backend/tsearch/spell.c:2071-2160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2071-L2160)

## Overview
Validates and applies an affix transformation to a word, checking compound word flags and performing pattern matching to generate the base word form.

## Definition
```c
static char *CheckAffix(const char *word, size_t len, AFFIX *Affix, int flagflags, char *newword, int *baselen)
```

## Detailed Description
CheckAffix performs affix checking and transformation as part of the spell checking process. It first validates compound word flags to ensure the affix is appropriate for the given context (beginning, middle, or end of compound words). Then it applies the affix transformation by replacing the affix portion with the base form specified in the AFFIX structure.

The function handles both prefix and suffix affixes differently - for suffixes, it replaces the end of the word, while for prefixes, it replaces the beginning. After transformation, it validates the result using either simple checking, regular expression matching, or wide character regex matching depending on the affix configuration.

## Parameters / Member Variables
- `word`: Input word to check and transform
- `len`: Length of the input word
- `Affix`: Affix structure containing transformation rules and flags
- `flagflags`: Compound word position flags (FF_COMPOUNDBEGIN, FF_COMPOUNDMIDDLE, etc.)
- `newword`: Output buffer for the transformed word
- `baselen`: Pointer to store the length of the unchanged part of the word

## Dependencies
- Functions called/Symbols referenced:
  - strcpy, strcat, strlen (string manipulation functions)
  - [RS_execute](../R/RS_execute.md) (for regex matching)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (multibyte to wide character conversion)
  - [pg_regexec](../p/pg_regexec.md) (PostgreSQL regex execution)
  - [palloc](../p/palloc.md), pfree (PostgreSQL memory management)
- Called from (representative examples):
  - [NormalizeSubWord](../N/NormalizeSubWord.md) (3 times at lines 2217, 2242, 2259)

## Notes and Other Information
- Returns the transformed word on success, NULL on failure
- Handles compound word flag validation (COMPOUNDONLY, COMPOUNDBEGIN, COMPOUNDMIDDLE, COMPOUNDLAST, COMPOUNDFORBIDFLAG)
- Supports three types of pattern matching: simple (no pattern), regis (simple regex), and full PostgreSQL regex
- Part of PostgreSQL's text search spell checking functionality
- The baselen parameter helps track word boundaries in compound word processing

## Simplified Source

```c
static char *
CheckAffix(const char *word, size_t len, AFFIX *Affix, int flagflags, char *newword, int *baselen)
{
    // Check compound word flags compatibility
    if (!validate_compound_flags(flagflags, Affix))
        return NULL;

    // Apply affix transformation
    if (Affix->type == FF_SUFFIX) {
        // Replace suffix: copy word and change ending
        strcpy(newword, word);
        strcpy(newword + len - Affix->replen, Affix->find);
        if (baselen)
            *baselen = len - Affix->replen;
    }
    else {
        // Replace prefix: check for valid base length
        if (baselen && *baselen + strlen(Affix->find) <= Affix->replen)
            return NULL;
        strcpy(newword, Affix->find);
        strcat(newword, word + Affix->replen);
    }

    // Validate the transformed word
    if (Affix->issimple) {
        return newword;  // No pattern matching needed
    }
    else if (Affix->isregis) {
        // Simple regex validation
        if (RS_execute(&(Affix->reg.regis), newword))
            return newword;
    }
    else {
        // Full regex validation with wide characters
        pg_wchar *data;
        int newword_len = strlen(newword);

        data = (pg_wchar *) palloc((newword_len + 1) * sizeof(pg_wchar));
        size_t data_len = pg_mb2wchar_with_len(newword, data, newword_len);

        if (pg_regexec(Affix->reg.pregex, data, data_len, 0, NULL, 0, NULL, 0) == REG_OKAY) {
            pfree(data);
            return newword;
        }
        pfree(data);
    }

    return NULL;
}
```