# find_compatible_trans

## Location
[src/backend/optimizer/prep/prepagg.c:458-521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepagg.c#L458-L521)

## Overview
Searches for a previously initialized transition state that can be shared with the current aggregate, enabling optimization where different aggregates can reuse the same transition computation.

## Definition
```c
static int find_compatible_trans(PlannerInfo *root, Aggref *newagg, bool shareable,
                                 Oid aggtransfn, Oid aggtranstype,
                                 int transtypeLen, bool transtypeByVal,
                                 Oid aggcombinefn,
                                 Oid aggserialfn, Oid aggdeserialfn,
                                 Datum initValue, bool initValueIsNull,
                                 List *transnos)
```

## Detailed Description
This function implements the second level of aggregate optimization by searching for existing transition states that can be shared between different aggregate functions. This optimization is crucial for cases like AVG(x) and STDDEV(x) which can share the same transition state computation but require different final functions.

The function performs comprehensive compatibility checking to ensure that transition state sharing is safe:

**Core Compatibility Requirements**:
1. **Transition function compatibility**: Both aggregates must use the same transition function (aggtransfn) and transition type (aggtranstype)
2. **Serialization compatibility**: Serialization and deserialization functions must match for proper partial aggregation support
3. **Combine function compatibility**: Combine functions must be identical for partial aggregation scenarios  
4. **Initial value compatibility**: Initial conditions must be identical (both null or same non-null values)
5. **Shareability**: The aggregate must be marked as shareable (final function is not read-write destructive)

**Optimization Scenarios**:
- **Different aggregates, same transition**: Functions like AVG(x) and STDDEV(x) that compute different final results but can share the same running totals during transition
- **Partial aggregation support**: Ensures that aggregates sharing transition state can still be properly combined and serialized for distributed/parallel execution

**Safety Checks**:
- Validates that serialization formats are compatible
- Ensures combine functions match for proper partial aggregation  
- Verifies initial values are identical using proper datum comparison
- Respects shareability constraints from final function analysis

The function searches through the provided list of candidate transition numbers (from aggregates with matching inputs) and returns the first compatible match, or -1 if no suitable transition state can be shared.

## Parameters / Member Variables
- : PlannerInfo structure containing aggtransinfos list of existing transition states
- : Aggref node for the aggregate seeking to share transition state
- : Boolean indicating if the aggregate's final function allows state sharing
- : OID of the transition function for the new aggregate
- : OID of the transition state data type
- : Length of transition type (for datum comparison)
- : Whether transition type is passed by value (for datum comparison)
- : OID of combine function for partial aggregation
- : OID of serialization function for parallel aggregation
- : OID of deserialization function for parallel aggregation  
- : Initial value for transition state
- : Whether the initial value is NULL
- : List of candidate transition numbers with compatible inputs

## Dependencies
- Functions called/Symbols referenced:
  - lfirst_int (list access macro)
  - list_nth_node (list access function)
  - [datumIsEqual](../d/datumIsEqual.md) (datum comparison function)
- Called from (representative examples):
  - [preprocess_aggref](../p/preprocess_aggref.md)

## Notes and Other Information
- This is a static function only accessible within the same source file
- Returns -1 if no compatible transition state is found, otherwise returns the transno of the compatible transition
- The function is conservative about partial aggregation compatibility, checking combine functions even when partial aggregation plans are not yet determined
- Serialization/deserialization function matching is critical for proper parallel execution
- Initial value comparison uses datumIsEqual for proper handling of different data types
- The shareable parameter acts as an early filter - non-shareable aggregates immediately return -1
- Searches through transnos list in order, returning the first match found
- InvalidOid values for serialization functions are handled correctly (both must be invalid to match)
- This function enables the optimization where different aggregate functions can share expensive transition state computation while maintaining separate final result calculation

## Simplified Source

```c
static int
find_compatible_trans(PlannerInfo *root, Aggref *newagg, bool shareable,
                      Oid aggtransfn, Oid aggtranstype,
                      int transtypeLen, bool transtypeByVal,
                      Oid aggcombinefn,
                      Oid aggserialfn, Oid aggdeserialfn,
                      Datum initValue, bool initValueIsNull,
                      List *transnos)
{
    ListCell *lc;

    // Only shareable aggregates can share transition state
    if (!shareable)
        return -1;

    // Search through candidate transition states
    foreach(lc, transnos)
    {
        int transno = lfirst_int(lc);
        AggTransInfo *pertrans = list_nth_node(AggTransInfo,
                                              root->aggtransinfos,
                                              transno);

        // Transition function and type must match
        if (aggtransfn != pertrans->transfn_oid ||
            aggtranstype != pertrans->aggtranstype)
            continue;

        // Serialization functions must match for parallel aggregation
        if (aggserialfn != pertrans->serialfn_oid ||
            aggdeserialfn != pertrans->deserialfn_oid)
            continue;

        // Combine function must match for partial aggregation
        if (aggcombinefn != pertrans->combinefn_oid)
            continue;

        // Initial values must be identical
        if (initValueIsNull && pertrans->initValueIsNull)
            return transno;

        if (!initValueIsNull && !pertrans->initValueIsNull &&
            datumIsEqual(initValue, pertrans->initValue,
                        transtypeByVal, transtypeLen))
            return transno;
    }

    return -1;  // No compatible transition state found
}
```