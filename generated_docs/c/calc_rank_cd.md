# calc_rank_cd

## Location
[src/backend/utils/adt/tsrank.c:850-952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsrank.c#L850-L952)

## Overview
Calculates text search ranking using cover density algorithm, evaluating how tightly query terms cluster together in the document with various normalization options.

## Definition
static float4 calc_rank_cd(const float4 *arrdata, TSVector txt, TSQuery query, int method)

## Detailed Description
This function implements the cover density ranking algorithm for PostgreSQL's text search functionality. It builds a document representation, finds all covers (minimal spans containing query terms), and calculates a ranking score based on cover density and proximity. The algorithm considers term weights, cover tightness, and distance between covers. Multiple normalization methods can be applied including document length, unique terms, logarithmic scaling, and extent distribution normalization.

## Parameters / Member Variables
- `arrdata`: Array of weight coefficients for different term categories (A, B, C, D)
- `txt`: TSVector containing the document with positional information
- `query`: TSQuery containing the search query terms and operators
- `method`: Bitmask specifying normalization methods to apply

## Dependencies
- Functions called/Symbols referenced:
  - TSVector (document vector type)
  - TSQuery (query type)
  - float4 (floating point return type)
  - DocRepresentation (document representation structure)
  - CoverExt (cover extension structure)
  - lengthof (array length macro)
  - [QueryRepresentation](../Q/QueryRepresentation.md) (query representation structure)
  - [QueryRepresentationOperand](../Q/QueryRepresentationOperand.md) (operand data structure)
  - [get_docrep](../g/get_docrep.md) (build document representation)
  - MemSet (memory initialization)
  - [Cover](../C/Cover.md) (find covers algorithm)
  - WEP_GETWEIGHT (extract term weight)
  - [cnt_length](cnt_length.md) (count document length)
  - RANK_NORM_* (normalization method constants)
- Called from (representative examples):
  - [ts_rankcd_wttf](../t/ts_rankcd_wttf.md) (called at line 961)
  - [ts_rankcd_wtt](../t/ts_rankcd_wtt.md) (called at line 977)
  - [ts_rankcd_ttf](../t/ts_rankcd_ttf.md) (called at line 993)
  - [ts_rankcd_tt](../t/ts_rankcd_tt.md) (called at line 1007)

## Notes and Other Information
This function is the core implementation of PostgreSQL's cover density ranking algorithm, which is considered more sophisticated than simple term frequency approaches. The algorithm finds minimal text spans containing all query terms and calculates density scores based on cover tightness and term weights. Multiple normalization methods are supported to handle different document characteristics. The function handles edge cases like overlapping covers and missing terms gracefully. Performance is optimized through efficient cover finding and memory management.

## Simplified Source

```c
static float4
calc_rank_cd(const float4 *arrdata, TSVector txt, TSQuery query, int method)
{
    DocRepresentation *doc;
    int len, doclen = 0;
    CoverExt ext;
    double Wdoc = 0.0;
    double invws[lengthof(weights)];
    double SumDist = 0.0, PrevExtPos = 0.0;
    int NExtent = 0;
    QueryRepresentation qr;

    // Initialize inverse weights and validate ranges
    for (int i = 0; i < lengthof(weights); i++) {
        invws[i] = (arrdata[i] >= 0) ? arrdata[i] : weights[i];
        if (invws[i] > 1.0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                           errmsg("weight out of range")));
        invws[i] = 1.0 / invws[i];
    }

    // Build query representation
    qr.query = query;
    qr.operandData = palloc0(sizeof(QueryRepresentationOperand) * query->size);

    // Get document representation with positions
    doc = get_docrep(txt, &qr, &doclen);
    if (!doc) {
        pfree(qr.operandData);
        return 0.0;
    }

    // Find covers and calculate ranking
    MemSet(&ext, 0, sizeof(CoverExt));
    while (Cover(doc, doclen, &qr, &ext)) {
        double Cpos = 0.0;
        double InvSum = 0.0;
        double CurExtPos;
        int nNoise;
        DocRepresentation *ptr = ext.begin;

        // Sum inverse weights for cover terms
        while (ptr <= ext.end) {
            InvSum += invws[WEP_GETWEIGHT(ptr->pos)];
            ptr++;
        }

        // Calculate cover density
        Cpos = ((double) (ext.end - ext.begin + 1)) / InvSum;

        // Estimate noise words in cover
        nNoise = (ext.q - ext.p) - (ext.end - ext.begin);
        if (nNoise < 0)
            nNoise = (ext.end - ext.begin) / 2;

        Wdoc += Cpos / ((double) (1 + nNoise));

        // Calculate distance between extents
        CurExtPos = ((double) (ext.q + ext.p)) / 2.0;
        if (NExtent > 0 && CurExtPos > PrevExtPos)
            SumDist += 1.0 / (CurExtPos - PrevExtPos);

        PrevExtPos = CurExtPos;
        NExtent++;
    }

    // Apply normalization methods
    if ((method & RANK_NORM_LOGLENGTH) && txt->size > 0)
        Wdoc /= log((double) (cnt_length(txt) + 1));

    if (method & RANK_NORM_LENGTH) {
        len = cnt_length(txt);
        if (len > 0)
            Wdoc /= (double) len;
    }

    if ((method & RANK_NORM_EXTDIST) && NExtent > 0 && SumDist > 0)
        Wdoc /= ((double) NExtent) / SumDist;

    if ((method & RANK_NORM_UNIQ) && txt->size > 0)
        Wdoc /= (double) (txt->size);

    if ((method & RANK_NORM_LOGUNIQ) && txt->size > 0)
        Wdoc /= log((double) (txt->size + 1)) / log(2.0);

    if (method & RANK_NORM_RDIVRPLUS1)
        Wdoc /= (Wdoc + 1);

    pfree(doc);
    pfree(qr.operandData);

    return (float4) Wdoc;
}
```