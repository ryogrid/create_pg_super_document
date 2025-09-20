# AggTransInfo

## Location
[src/include/nodes/pathnodes.h:3399-3435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/pathnodes.h#L3399-L3435)

## Overview
AggTransInfo holds information about transition state that is shared by one or more aggregate functions in a query, enabling optimization through transition state sharing when aggregates have identical inputs and transition functions.

## Definition

```c
typedef struct AggTransInfo
{
	pg_node_attr(no_copy_equal, no_read, no_query_jumble)

	NodeTag		type;

	/* Inputs for this transition state */
	List	   *args;
	Expr	   *aggfilter;

	/* Oid of the state transition function */
	Oid			transfn_oid;

	/* Oid of the serialization function, or InvalidOid if none */
	Oid			serialfn_oid;

	/* Oid of the deserialization function, or InvalidOid if none */
	Oid			deserialfn_oid;

	/* Oid of the combine function, or InvalidOid if none */
	Oid			combinefn_oid;

	/* Oid of state value's datatype */
	Oid			aggtranstype;

	/* Additional data about transtype */
	int32		aggtranstypmod;
	int			transtypeLen;
	bool		transtypeByVal;

	/* Space-consumption estimate */
	int32		aggtransspace;

	/* Initial value from pg_aggregate entry */
	Datum		initValue pg_node_attr(read_write_ignore);
	bool		initValueIsNull;
} AggTransInfo;
```
## Detailed Description
AggTransInfo is a critical optimization structure in PostgreSQL's aggregate processing system. It enables multiple aggregate functions to share the same transition state when they have identical inputs and transition functions, reducing computational overhead and memory usage. Each unique combination of inputs and transition function gets assigned a single AggTransInfo, and all aggregates sharing these characteristics reference the same 'aggtransno' value. The structure contains comprehensive metadata about the transition state, including serialization/deserialization functions for parallel aggregation, combine functions for partial aggregation, and space consumption estimates for memory management.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an AggTransInfo node
- `*args`: List of input arguments for this transition state
- `*aggfilter`: Filter expression applied to the aggregate inputs, or NULL if no filter
- `transfn_oid`: OID of the state transition function that processes each input row
- `serialfn_oid`: OID of the serialization function for parallel aggregation, or InvalidOid if not used
- `deserialfn_oid`: OID of the deserialization function for parallel aggregation, or InvalidOid if not used
- `combinefn_oid`: OID of the combine function for merging partial states, or InvalidOid if not used
- `aggtranstype`: OID of the datatype used for the transition state value
- `aggtranstypmod`: Type modifier for the transition state datatype
- `transtypeLen`: Length of the transition state datatype (-1 for variable length)
- `transtypeByVal`: Boolean indicating if the transition state is passed by value or by reference
- `aggtransspace`: Estimated space consumption in bytes for this transition state
- `pg_node_attr(read_write_ignore)`: Initial value for the transition state from pg_aggregate catalog entry
- `initValueIsNull`: Boolean indicating if the initial value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [initValue](../i/initValue.md)
- Called from (representative examples):
  - [preprocess_aggref](../p/preprocess_aggref.md)
  - [find_compatible_trans](../f/find_compatible_trans.md)
  - [get_agg_clause_costs](../g/get_agg_clause_costs.md)

## Notes and Other Information
- This structure is fundamental to PostgreSQL's aggregate optimization strategy, enabling efficient resource sharing
- The pg_node_attr annotations control how the structure is handled during node copying, equality checking, and query jumbling
- Multiple Aggref nodes can reference the same AggTransInfo through identical aggtransno values
- The serialization/deserialization functions are used in parallel query execution to transfer state between workers
- The combine function enables partial aggregation in distributed or parallel contexts
- Space estimation is crucial for memory management and query planning decisions
- The structure supports both simple aggregates and complex aggregates with filters and custom state management
- Located in src/include/nodes/pathnodes.h:3399-3435