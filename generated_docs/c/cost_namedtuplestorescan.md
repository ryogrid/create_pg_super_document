# cost_namedtuplestorescan

## Location
[src/backend/optimizer/path/costsize.c:1739-1775](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L1739-L1775)

## Overview
Determines and returns the cost of scanning a named tuplestore, which represents materialized data stored in memory that can be accessed by name.

## Definition

```c
void
cost_namedtuplestorescan(Path *path, PlannerInfo *root,
						 RelOptInfo *baserel, ParamPathInfo *param_info)
```
## Detailed Description
This function calculates the execution cost for scanning a named tuplestore. Named tuplestores are used in PostgreSQL to store materialized results that can be referenced by name, commonly used in features like:

1. **Materialized results**: Data that has been computed and stored for repeated access
2. **Tuplestore manipulation**: The cost model accounts for the overhead of accessing data from the tuplestore structure
3. **Qualification processing**: Includes costs for applying any restriction qualifiers (WHERE clauses)
4. **Row estimation**: Uses parameterized path information when available

The cost calculation is similar to other scan types but specifically accounts for the overhead of tuplestore access patterns.

## Parameters / Member Variables
- : The Path node to store the calculated costs (startup_cost and total_cost fields are set)
- : PlannerInfo structure containing global planning information and cost parameters
- : RelOptInfo representing the named tuplestore relation (must have rtekind == RTE_NAMEDTUPLESTORE)
- : ParamPathInfo for parameterized paths, or NULL for non-parameterized scans

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_qual_cost](../g/get_restriction_qual_cost.md) (calculates cost of applying restriction qualifiers)
  - cpu_tuple_cost (global cost parameter for tuple processing)
- Types referenced:
  - [ParamPathInfo](../P/ParamPathInfo.md) (parameterized path information)
  - Cost (cost calculation type)
  - [QualCost](../Q/QualCost.md) (qualification cost structure)
  - RTE_NAMEDTUPLESTORE (enum value for named tuplestore range table entries)
- Called from:
  - [create_namedtuplestorescan_path](create_namedtuplestorescan_path.md) (in pathnode.c:2165)

## Notes and Other Information
- The function includes an assertion ensuring the relation is a named tuplestore (rtekind == RTE_NAMEDTUPLESTORE)
- Uses double cpu_tuple_cost: once for tuplestore manipulation overhead, once for standard tuple processing
- Does not include target list evaluation costs, unlike some other scan cost functions, suggesting the target list processing might be handled elsewhere or be minimal for named tuplestores
- The cost model treats named tuplestores as in-memory structures, so no I/O costs are included
- Designed for scanning pre-materialized data that can be accessed efficiently by name reference

## Simplified Source
```c
void
cost_namedtuplestorescan(Path *path, PlannerInfo *root,
                        RelOptInfo *baserel, ParamPathInfo *param_info)
{
    Cost startup_cost = 0;
    Cost run_cost = 0;
    QualCost qual_cost;
    Cost cpu_per_tuple;

    // Validate this is a named tuplestore relation
    Assert(baserel->relid > 0);
    Assert(baserel->rtekind == RTE_NAMEDTUPLESTORE);

    // Set row estimate (parameterized or base estimate)
    if (param_info)
        path->rows = param_info->ppi_rows;
    else
        path->rows = baserel->rows;

    // Base cost: one CPU tuple cost for tuplestore access
    cpu_per_tuple = cpu_tuple_cost;

    // Add qualification costs (WHERE clauses)
    get_restriction_qual_cost(root, baserel, param_info, &qual_cost);
    startup_cost += qual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost + qual_cost.per_tuple;

    // Total scan cost = cpu cost × number of tuples
    run_cost += cpu_per_tuple * baserel->tuples;

    // Set final costs
    path->startup_cost = startup_cost;
    path->total_cost = startup_cost + run_cost;
}
```