# compute_tsvector_stats

## Location
[src/backend/tsearch/ts_typanalyze.c:141-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/ts_typanalyze.c#L141-L452)

## Overview
Computes statistics for tsvector columns using the Lossy Counting algorithm to identify the most common lexemes and their frequencies for query selectivity estimation.

## Definition

```c
struct a hash key.  The key points into the (detoasted)
			 * tsvector value at this point, but if a new entry is created, we
			 * make a copy of it.  This way we can free the tsvector value
			 * once we've processed all its lexemes.
			 */
			hash_key.lexeme = lexemesptr + curentryptr->pos;
```
## Detailed Description
This function implements statistics collection for tsvector columns by finding the most common lexemes rather than most common values (since tsvectors are typically unique). It uses the Lossy Counting algorithm from Manku and Motwani to efficiently track lexeme frequencies in a streaming fashion. The algorithm maintains a hash table of lexemes with their frequencies and periodically prunes low-frequency entries. The resulting statistics are stored in the MCELEM slot of pg_statistic to support @@ operator selectivity estimation.

## Parameters / Member Variables
- : VacAttrStats structure to populate with computed statistics
- : Function to fetch sample values from the column
- : Number of rows in the sample
- : Total number of rows in the table (unused in this implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [VacAttrStats](../V/VacAttrStats.md)
  - [HTAB](../H/HTAB.md)
  - [HASHCTL](../H/HASHCTL.md)
  - LexemeHashKey
  - [TrackItem](../T/TrackItem.md)
  - [lexeme_hash](../l/lexeme_hash.md)
  - [lexeme_match](../l/lexeme_match.md)
  - [prune_lexemes_hashtable](../p/prune_lexemes_hashtable.md)
  - [hash_create](../h/hash_create.md)
  - TSVector
  - [WordEntry](../W/WordEntry.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - VARSIZE_ANY
  - [DatumGetTSVector](../D/DatumGetTSVector.md)
  - STRPTR
  - ARRPTR
  - [hash_search](../h/hash_search.md)
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [trackitem_compare_frequencies_desc](../t/trackitem_compare_frequencies_desc.md)
  - [trackitem_compare_lexemes](../t/trackitem_compare_lexemes.md)
  - [cstring_to_text_with_len](cstring_to_text_with_len.md)
- Called from (representative examples):
  - [ts_typanalyze](../t/ts_typanalyze.md) (via function pointer assignment)

## Notes and Other Information
- Uses Lossy Counting algorithm with bucket width = (num_mcelem + 10) * 1000 / 7
- Assumes tsvector columns are unique (stadistinct = -1.0)
- Target is statistics_target * 10 lexemes in MCELEM array
- Stores lexemes sorted by length then lexicographically for binary search efficiency
- Includes min/max frequencies in extra mcelem_freqs slots
- Frequency calculations are relative to non-null row count, not total lexeme count
- Based on Zipfian distribution assumptions for natural language lexeme frequencies

## Simplified Source

```c
static void compute_tsvector_stats(VacAttrStats *stats,
                                 AnalyzeAttrFetchFunc fetchfunc,
                                 int samplerows,
                                 double totalrows) {
    int num_mcelem = stats->attstattarget * 10;
    int bucket_width = (num_mcelem + 10) * 1000 / 7;
    int null_cnt = 0;
    double total_width = 0;
    HTAB *lexemes_tab;
    HASHCTL hash_ctl;
    int b_current = 1;
    int lexeme_no = 0;
    LexemeHashKey hash_key;

    // Create hash table for tracking lexemes
    hash_ctl.keysize = sizeof(LexemeHashKey);
    hash_ctl.entrysize = sizeof(TrackItem);
    hash_ctl.hash = lexeme_hash;
    hash_ctl.match = lexeme_match;
    hash_ctl.hcxt = CurrentMemoryContext;
    lexemes_tab = hash_create("Analyzed lexemes table", num_mcelem, &hash_ctl,
                             HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

    // Process each tsvector sample
    for (int vector_no = 0; vector_no < samplerows; vector_no++) {
        Datum value;
        bool isnull;
        TSVector vector;

        vacuum_delay_point();
        value = fetchfunc(stats, vector_no, &isnull);

        if (isnull) {
            null_cnt++;
            continue;
        }

        total_width += VARSIZE_ANY(DatumGetPointer(value));
        vector = DatumGetTSVector(value);

        // Process each lexeme in the tsvector
        char *lexemesptr = STRPTR(vector);
        WordEntry *curentryptr = ARRPTR(vector);
        for (int j = 0; j < vector->size; j++) {
            TrackItem *item;
            bool found;

            hash_key.lexeme = lexemesptr + curentryptr->pos;
            hash_key.length = curentryptr->len;

            // Add or update lexeme frequency
            item = hash_search(lexemes_tab, &hash_key, HASH_ENTER, &found);
            if (found) {
                item->frequency++;
            } else {
                item->frequency = 1;
                item->delta = b_current - 1;
                item->key.lexeme = palloc(hash_key.length);
                memcpy(item->key.lexeme, hash_key.lexeme, hash_key.length);
            }

            lexeme_no++;

            // Prune hashtable after each bucket
            if (lexeme_no % bucket_width == 0) {
                prune_lexemes_hashtable(lexemes_tab, b_current);
                b_current++;
            }

            curentryptr++;
        }

        if (TSVectorGetDatum(vector) != value)
            pfree(vector);
    }

    // Generate final statistics
    if (null_cnt < samplerows) {
        int nonnull_cnt = samplerows - null_cnt;
        int cutoff_freq = 9 * lexeme_no / bucket_width;
        TrackItem **sort_table;
        int track_len = 0;
        int minfreq = lexeme_no, maxfreq = 0;

        // Collect qualifying entries
        HASH_SEQ_STATUS scan_status;
        TrackItem *item;
        int i = hash_get_num_entries(lexemes_tab);
        sort_table = palloc(sizeof(TrackItem *) * i);

        hash_seq_init(&scan_status, lexemes_tab);
        while ((item = hash_seq_search(&scan_status)) != NULL) {
            if (item->frequency > cutoff_freq) {
                sort_table[track_len++] = item;
                minfreq = Min(minfreq, item->frequency);
                maxfreq = Max(maxfreq, item->frequency);
            }
        }

        // Sort and store results
        if (num_mcelem < track_len) {
            qsort_interruptible(sort_table, track_len, sizeof(TrackItem *),
                              trackitem_compare_frequencies_desc, NULL);
            minfreq = sort_table[num_mcelem - 1]->frequency;
        } else {
            num_mcelem = track_len;
        }

        if (num_mcelem > 0) {
            qsort_interruptible(sort_table, num_mcelem, sizeof(TrackItem *),
                              trackitem_compare_lexemes, NULL);

            // Store statistics
            Datum *mcelem_values = palloc(num_mcelem * sizeof(Datum));
            float4 *mcelem_freqs = palloc((num_mcelem + 2) * sizeof(float4));

            for (i = 0; i < num_mcelem; i++) {
                TrackItem *titem = sort_table[i];
                mcelem_values[i] = PointerGetDatum(cstring_to_text_with_len(
                    titem->key.lexeme, titem->key.length));
                mcelem_freqs[i] = (double) titem->frequency / (double) nonnull_cnt;
            }
            mcelem_freqs[i++] = (double) minfreq / (double) nonnull_cnt;
            mcelem_freqs[i] = (double) maxfreq / (double) nonnull_cnt;

            stats->stakind[0] = STATISTIC_KIND_MCELEM;
            stats->stanumbers[0] = mcelem_freqs;
            stats->numnumbers[0] = num_mcelem + 2;
            stats->stavalues[0] = mcelem_values;
            stats->numvalues[0] = num_mcelem;
        }

        stats->stats_valid = true;
        stats->stanullfrac = (double) null_cnt / (double) samplerows;
        stats->stawidth = total_width / (double) nonnull_cnt;
        stats->stadistinct = -1.0 * (1.0 - stats->stanullfrac);
    } else {
        stats->stats_valid = true;
        stats->stanullfrac = 1.0;
        stats->stawidth = 0;
        stats->stadistinct = 0.0;
    }
}
```