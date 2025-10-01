# replace_text_regexp

## Location
[src/backend/utils/adt/varlena.c:4206-4367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4206-L4367)

## Overview
The core function that implements regular expression-based text replacement in PostgreSQL, supporting pattern matching with capture groups and sophisticated replacement text processing.

## Definition

```c
text *
replace_text_regexp(text *src_text, text *pattern_text,
					text *replace_text,
					int cflags, Oid collation,
					int search_start, int n)
```
## Detailed Description
This function performs regular expression search and replace operations on text strings. It supports advanced features including:

- **Pattern matching**: Uses POSIX regular expressions with configurable compilation flags
- **Capture groups**: Supports up to 9 numbered capture groups (\1-\9) plus the full match (\&)
- **Selective replacement**: Can replace all matches or just the N-th occurrence
- **Unicode support**: Properly handles multi-byte character encodings
- **Performance optimization**: Uses REG_NOSUB when replacement text contains no back-references

The function operates by converting the source text to wide characters for proper regex processing, then iteratively finding matches and building the result string by copying non-matching segments and processed replacement text.

## Parameters / Member Variables
- : Source text to search for pattern matches
- : Regular expression pattern to match against  
- : Replacement text that may contain back-references and escape sequences
- : Regular expression compilation flags (e.g., case sensitivity options)
- : Text collation to use for pattern matching
- : Character offset in src_text where searching should begin
- : If 0, replace all matches; if > 0, replace only the N-th match

## Dependencies
- Functions called/Symbols referenced:
  - [check_replace_text_has_escape](../c/check_replace_text_has_escape.md) (analyze replacement text for optimization)
  - [appendStringInfoRegexpSubstr](../a/appendStringInfoRegexpSubstr.md) (process replacement text with back-references)
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md) (compile and cache regex pattern)
  - [pg_regexec](../p/pg_regexec.md) (execute regex search)
  - [pg_mb2wchar_with_len](../p/pg_mb2wchar_with_len.md) (convert multibyte text to wide characters)
  - [charlen_to_bytelen](../c/charlen_to_bytelen.md) (convert character positions to byte positions)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (append binary data to result buffer)
  - [appendStringInfoText](../a/appendStringInfoText.md) (append text without processing)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (convert C string result to PostgreSQL text)
- Called from (representative examples):
  - [textregexreplace_noopt](../t/textregexreplace_noopt.md)
  - [textregexreplace](../t/textregexreplace.md)  
  - [textregexreplace_extended](../t/textregexreplace_extended.md)

## Notes and Other Information
- This is a public function exported via varlena.h for use by regexp functions
- Handles zero-length matches correctly by advancing search position
- Automatically optimizes performance by using REG_NOSUB when no capture groups are needed
- Supports interruption via CHECK_FOR_INTERRUPTS() for long-running operations
- Located in src/backend/utils/adt/varlena.c:4206-4367
- Memory management includes proper cleanup of allocated buffers and wide character arrays

## Simplified Source

```c
text *
replace_text_regexp(text *src_text, text *pattern_text,
                    text *replace_text,
                    int cflags, Oid collation,
                    int search_start, int n)
{
    text *ret_text;
    regex_t *re;
    int src_text_len = VARSIZE_ANY_EXHDR(src_text);
    int nmatches = 0;
    StringInfoData buf;
    regmatch_t pmatch[10];  // main match plus \1 to \9
    int nmatch = lengthof(pmatch);
    pg_wchar *data;
    size_t data_len;
    int data_pos;
    char *start_ptr;

    initStringInfo(&buf);

    // Convert text to wide characters for regex processing
    data = (pg_wchar *) palloc((src_text_len + 1) * sizeof(pg_wchar));
    data_len = pg_mb2wchar_with_len(VARDATA_ANY(src_text), data, src_text_len);

    // Check if replacement text has escape sequences
    int escape_status = check_replace_text_has_escape(replace_text);
    if (escape_status < 2)
    {
        cflags |= REG_NOSUB;  // Optimization: no capture groups needed
        nmatch = 1;
    }

    // Compile regex pattern
    re = RE_compile_and_cache(pattern_text, cflags, collation);

    start_ptr = (char *) VARDATA_ANY(src_text);
    data_pos = 0;

    // Main replacement loop
    while (search_start <= data_len)
    {
        CHECK_FOR_INTERRUPTS();

        // Search for next match
        int regexec_result = pg_regexec(re, data, data_len, search_start,
                                        NULL, nmatch, pmatch, 0);

        if (regexec_result == REG_NOMATCH)
            break;

        if (regexec_result != REG_OKAY)
            ereport(ERROR, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                           errmsg("regular expression failed")));

        nmatches++;

        // Skip this match if we only want the N-th occurrence
        if (n > 0 && nmatches != n)
        {
            search_start = pmatch[0].rm_eo;
            if (pmatch[0].rm_so == pmatch[0].rm_eo)
                search_start++;
            continue;
        }

        // Copy text before match
        if (pmatch[0].rm_so - data_pos > 0)
        {
            int chunk_len = charlen_to_bytelen(start_ptr, pmatch[0].rm_so - data_pos);
            appendBinaryStringInfo(&buf, start_ptr, chunk_len);
            start_ptr += chunk_len;
            data_pos = pmatch[0].rm_so;
        }

        // Process replacement text (with escape sequences if present)
        if (escape_status > 0)
            appendStringInfoRegexpSubstr(&buf, replace_text, pmatch,
                                         start_ptr, data_pos);
        else
            appendStringInfoText(&buf, replace_text);

        // Advance past the match
        start_ptr += charlen_to_bytelen(start_ptr, pmatch[0].rm_eo - data_pos);
        data_pos = pmatch[0].rm_eo;

        // Stop if only replacing one occurrence
        if (n > 0)
            break;

        // Continue searching from end of match
        search_start = data_pos;
        if (pmatch[0].rm_so == pmatch[0].rm_eo)
            search_start++;
    }

    // Copy remaining text after last match
    if (data_pos < data_len)
    {
        int chunk_len = ((char *) src_text + VARSIZE_ANY(src_text)) - start_ptr;
        appendBinaryStringInfo(&buf, start_ptr, chunk_len);
    }

    // Convert result back to text and cleanup
    ret_text = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data);
    pfree(data);

    return ret_text;
}
```