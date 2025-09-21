9.21. Aggregate Functions  
---  
[Prev](functions-range.md "9.20. Range/Multirange Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-window.md "9.22. Window Functions")  
  
* * *

## 9.21. Aggregate Functions #

_Aggregate functions_ compute a single result from a set of input values. The built-in general-purpose aggregate functions are listed in [Table 9.60](functions-aggregate.md#FUNCTIONS-AGGREGATE-TABLE "Table 9.60. General-Purpose Aggregate Functions") while statistical aggregates are in [Table 9.61](functions-aggregate.md#FUNCTIONS-AGGREGATE-STATISTICS-TABLE "Table 9.61. Aggregate Functions for Statistics"). The built-in within-group ordered-set aggregate functions are listed in [Table 9.62](functions-aggregate.md#FUNCTIONS-ORDEREDSET-TABLE "Table 9.62. Ordered-Set Aggregate Functions") while the built-in within-group hypothetical-set ones are in [Table 9.63](functions-aggregate.md#FUNCTIONS-HYPOTHETICAL-TABLE "Table 9.63. Hypothetical-Set Aggregate Functions"). Grouping operations, which are closely related to aggregate functions, are listed in [Table 9.64](functions-aggregate.md#FUNCTIONS-GROUPING-TABLE "Table 9.64. Grouping Operations"). The special syntax considerations for aggregate functions are explained in [Section 4.2.7](sql-expressions.md#SYNTAX-AGGREGATES "4.2.7. Aggregate Expressions"). Consult [Section 2.7](tutorial-agg.md "2.7. Aggregate Functions") for additional introductory information. 

Aggregate functions that support _Partial Mode_ are eligible to participate in various optimizations, such as parallel aggregation. 

While all aggregates below accept an optional `ORDER BY` clause (as outlined in [Section 4.2.7](sql-expressions.md#SYNTAX-AGGREGATES "4.2.7. Aggregate Expressions")), the clause has only been added to aggregates whose output is affected by ordering. 

**Table 9.60. General-Purpose Aggregate Functions**

Function  Description | Partial Mode  
---|---  
`any_value` ( `anyelement` ) → `_`same as input type`_` Returns an arbitrary value from the non-null input values. | Yes  
`array_agg` ( `anynonarray` `ORDER BY` `input_sort_columns` ) → `anyarray` Collects all the input values, including nulls, into an array. | Yes  
`array_agg` ( `anyarray` `ORDER BY` `input_sort_columns` ) → `anyarray` Concatenates all the input arrays into an array of one higher dimension. (The inputs must all have the same dimensionality, and cannot be empty or null.) | Yes  
`avg` ( `smallint` ) → `numeric` `avg` ( `integer` ) → `numeric` `avg` ( `bigint` ) → `numeric` `avg` ( `numeric` ) → `numeric` `avg` ( `real` ) → `double precision` `avg` ( `double precision` ) → `double precision` `avg` ( `interval` ) → `interval` Computes the average (arithmetic mean) of all the non-null input values. | Yes  
`bit_and` ( `smallint` ) → `smallint` `bit_and` ( `integer` ) → `integer` `bit_and` ( `bigint` ) → `bigint` `bit_and` ( `bit` ) → `bit` Computes the bitwise AND of all non-null input values. | Yes  
`bit_or` ( `smallint` ) → `smallint` `bit_or` ( `integer` ) → `integer` `bit_or` ( `bigint` ) → `bigint` `bit_or` ( `bit` ) → `bit` Computes the bitwise OR of all non-null input values. | Yes  
`bit_xor` ( `smallint` ) → `smallint` `bit_xor` ( `integer` ) → `integer` `bit_xor` ( `bigint` ) → `bigint` `bit_xor` ( `bit` ) → `bit` Computes the bitwise exclusive OR of all non-null input values. Can be useful as a checksum for an unordered set of values. | Yes  
`bool_and` ( `boolean` ) → `boolean` Returns true if all non-null input values are true, otherwise false. | Yes  
`bool_or` ( `boolean` ) → `boolean` Returns true if any non-null input value is true, otherwise false. | Yes  
`count` ( `*` ) → `bigint` Computes the number of input rows. | Yes  
`count` ( `"any"` ) → `bigint` Computes the number of input rows in which the input value is not null. | Yes  
`every` ( `boolean` ) → `boolean` This is the SQL standard's equivalent to `bool_and`. | Yes  
`json_agg` ( `anyelement` `ORDER BY` `input_sort_columns` ) → `json` `jsonb_agg` ( `anyelement` `ORDER BY` `input_sort_columns` ) → `jsonb` Collects all the input values, including nulls, into a JSON array. Values are converted to JSON as per `to_json` or `to_jsonb`. | No  
`json_agg_strict` ( `anyelement` ) → `json` `jsonb_agg_strict` ( `anyelement` ) → `jsonb` Collects all the input values, skipping nulls, into a JSON array. Values are converted to JSON as per `to_json` or `to_jsonb`. | No  
`json_arrayagg` ( [ _`value_expression`_ ] [ `ORDER BY` _`sort_expression`_ ] [ { `NULL` | `ABSENT` } `ON NULL` ] [ `RETURNING` _`data_type`_ [ `FORMAT JSON` [ `ENCODING UTF8` ] ] ])  Behaves in the same way as `json_array` but as an aggregate function so it only takes one _`value_expression`_ parameter. If `ABSENT ON NULL` is specified, any NULL values are omitted. If `ORDER BY` is specified, the elements will appear in the array in that order rather than in the input order.  `SELECT json_arrayagg(v) FROM (VALUES(2),(1)) t(v)` → `[2, 1]` | No  
`json_objectagg` ( [ { _`key_expression`_ { `VALUE` | ':' } _`value_expression`_ } ] [ { `NULL` | `ABSENT` } `ON NULL` ] [ { `WITH` | `WITHOUT` } `UNIQUE` [ `KEYS` ] ] [ `RETURNING` _`data_type`_ [ `FORMAT JSON` [ `ENCODING UTF8` ] ] ])  Behaves like `json_object`, but as an aggregate function, so it only takes one _`key_expression`_ and one _`value_expression`_ parameter.  `SELECT json_objectagg(k:v) FROM (VALUES ('a'::text,current_date),('b',current_date + 1)) AS t(k,v)` → `{ "a" : "2022-05-10", "b" : "2022-05-11" }` | No  
`json_object_agg` ( _`key`_ `"any"`, _`value`_ `"any"` `ORDER BY` `input_sort_columns` ) → `json` `jsonb_object_agg` ( _`key`_ `"any"`, _`value`_ `"any"` `ORDER BY` `input_sort_columns` ) → `jsonb` Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per `to_json` or `to_jsonb`. Values can be null, but keys cannot. | No  
`json_object_agg_strict` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `json` `jsonb_object_agg_strict` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `jsonb` Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per `to_json` or `to_jsonb`. The _`key`_ can not be null. If the _`value`_ is null then the entry is skipped, | No  
`json_object_agg_unique` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `json` `jsonb_object_agg_unique` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `jsonb` Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per `to_json` or `to_jsonb`. Values can be null, but keys cannot. If there is a duplicate key an error is thrown. | No  
`json_object_agg_unique_strict` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `json` `jsonb_object_agg_unique_strict` ( _`key`_ `"any"`, _`value`_ `"any"` ) → `jsonb` Collects all the key/value pairs into a JSON object. Key arguments are coerced to text; value arguments are converted as per `to_json` or `to_jsonb`. The _`key`_ can not be null. If the _`value`_ is null then the entry is skipped. If there is a duplicate key an error is thrown. | No  
`max` ( _`see text`_ ) → `_`same as input type`_` Computes the maximum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as `inet`, `interval`, `money`, `oid`, `pg_lsn`, `tid`, `xid8`, and arrays of any of these types. | Yes  
`min` ( _`see text`_ ) → `_`same as input type`_` Computes the minimum of the non-null input values. Available for any numeric, string, date/time, or enum type, as well as `inet`, `interval`, `money`, `oid`, `pg_lsn`, `tid`, `xid8`, and arrays of any of these types. | Yes  
`range_agg` ( _`value`_ `anyrange` ) → `anymultirange` `range_agg` ( _`value`_ `anymultirange` ) → `anymultirange` Computes the union of the non-null input values. | No  
`range_intersect_agg` ( _`value`_ `anyrange` ) → `anyrange` `range_intersect_agg` ( _`value`_ `anymultirange` ) → `anymultirange` Computes the intersection of the non-null input values. | No  
`string_agg` ( _`value`_ `text`, _`delimiter`_ `text` ) → `text` `string_agg` ( _`value`_ `bytea`, _`delimiter`_ `bytea` `ORDER BY` `input_sort_columns` ) → `bytea` Concatenates the non-null input values into a string. Each value after the first is preceded by the corresponding _`delimiter`_ (if it's not null). | Yes  
`sum` ( `smallint` ) → `bigint` `sum` ( `integer` ) → `bigint` `sum` ( `bigint` ) → `numeric` `sum` ( `numeric` ) → `numeric` `sum` ( `real` ) → `real` `sum` ( `double precision` ) → `double precision` `sum` ( `interval` ) → `interval` `sum` ( `money` ) → `money` Computes the sum of the non-null input values. | Yes  
`xmlagg` ( `xml` `ORDER BY` `input_sort_columns` ) → `xml` Concatenates the non-null XML input values (see [Section 9.15.1.8](functions-xml.md#FUNCTIONS-XML-XMLAGG "9.15.1.8. xmlagg")). | No  
  
  


It should be noted that except for `count`, these functions return a null value when no rows are selected. In particular, `sum` of no rows returns null, not zero as one might expect, and `array_agg` returns null rather than an empty array when there are no input rows. The `coalesce` function can be used to substitute zero or an empty array for null when necessary. 

The aggregate functions `array_agg`, `json_agg`, `jsonb_agg`, `json_agg_strict`, `jsonb_agg_strict`, `json_object_agg`, `jsonb_object_agg`, `json_object_agg_strict`, `jsonb_object_agg_strict`, `json_object_agg_unique`, `jsonb_object_agg_unique`, `json_object_agg_unique_strict`, `jsonb_object_agg_unique_strict`, `string_agg`, and `xmlagg`, as well as similar user-defined aggregate functions, produce meaningfully different result values depending on the order of the input values. This ordering is unspecified by default, but can be controlled by writing an `ORDER BY` clause within the aggregate call, as shown in [Section 4.2.7](sql-expressions.md#SYNTAX-AGGREGATES "4.2.7. Aggregate Expressions"). Alternatively, supplying the input values from a sorted subquery will usually work. For example: 
    
    
    SELECT xmlagg(x) FROM (SELECT x FROM test ORDER BY y DESC) AS tab;
    

Beware that this approach can fail if the outer query level contains additional processing, such as a join, because that might cause the subquery's output to be reordered before the aggregate is computed. 

### Note

The boolean aggregates `bool_and` and `bool_or` correspond to the standard SQL aggregates `every` and `any` or `some`. PostgreSQL supports `every`, but not `any` or `some`, because there is an ambiguity built into the standard syntax: 
    
    
    SELECT b1 = ANY((SELECT b2 FROM t2 ...)) FROM t1 ...;
    

Here `ANY` can be considered either as introducing a subquery, or as being an aggregate function, if the subquery returns one row with a Boolean value. Thus the standard name cannot be given to these aggregates. 

### Note

Users accustomed to working with other SQL database management systems might be disappointed by the performance of the `count` aggregate when it is applied to the entire table. A query like: 
    
    
    SELECT count(*) FROM sometable;
    

will require effort proportional to the size of the table: PostgreSQL will need to scan either the entire table or the entirety of an index that includes all rows in the table. 

[Table 9.61](functions-aggregate.md#FUNCTIONS-AGGREGATE-STATISTICS-TABLE "Table 9.61. Aggregate Functions for Statistics") shows aggregate functions typically used in statistical analysis. (These are separated out merely to avoid cluttering the listing of more-commonly-used aggregates.) Functions shown as accepting _`numeric_type`_ are available for all the types `smallint`, `integer`, `bigint`, `numeric`, `real`, and `double precision`. Where the description mentions _`N`_ , it means the number of input rows for which all the input expressions are non-null. In all cases, null is returned if the computation is meaningless, for example when _`N`_ is zero. 

**Table 9.61. Aggregate Functions for Statistics**

Function  Description | Partial Mode  
---|---  
`corr` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the correlation coefficient. | Yes  
`covar_pop` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the population covariance. | Yes  
`covar_samp` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the sample covariance. | Yes  
`regr_avgx` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the average of the independent variable, `sum(_`X`_)/_`N`_`. | Yes  
`regr_avgy` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the average of the dependent variable, `sum(_`Y`_)/_`N`_`. | Yes  
`regr_count` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `bigint` Computes the number of rows in which both inputs are non-null. | Yes  
`regr_intercept` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the y-intercept of the least-squares-fit linear equation determined by the (_`X`_ , _`Y`_) pairs. | Yes  
`regr_r2` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the square of the correlation coefficient. | Yes  
`regr_slope` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the slope of the least-squares-fit linear equation determined by the (_`X`_ , _`Y`_) pairs. | Yes  
`regr_sxx` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the “sum of squares” of the independent variable, `sum(_`X`_ ^2) - sum(_`X`_)^2/_`N`_`. | Yes  
`regr_sxy` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the “sum of products” of independent times dependent variables, `sum(_`X`_ *_`Y`_) - sum(_`X`_) * sum(_`Y`_)/_`N`_`. | Yes  
`regr_syy` ( _`Y`_ `double precision`, _`X`_ `double precision` ) → `double precision` Computes the “sum of squares” of the dependent variable, `sum(_`Y`_ ^2) - sum(_`Y`_)^2/_`N`_`. | Yes  
`stddev` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` This is a historical alias for `stddev_samp`. | Yes  
`stddev_pop` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` Computes the population standard deviation of the input values. | Yes  
`stddev_samp` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` Computes the sample standard deviation of the input values. | Yes  
`variance` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` This is a historical alias for `var_samp`. | Yes  
`var_pop` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` Computes the population variance of the input values (square of the population standard deviation). | Yes  
`var_samp` ( _`numeric_type`_ ) → `` `double precision` for `real` or `double precision`, otherwise `numeric` Computes the sample variance of the input values (square of the sample standard deviation). | Yes  
  
  


[Table 9.62](functions-aggregate.md#FUNCTIONS-ORDEREDSET-TABLE "Table 9.62. Ordered-Set Aggregate Functions") shows some aggregate functions that use the _ordered-set aggregate_ syntax. These functions are sometimes referred to as “inverse distribution” functions. Their aggregated input is introduced by `ORDER BY`, and they may also take a _direct argument_ that is not aggregated, but is computed only once. All these functions ignore null values in their aggregated input. For those that take a _`fraction`_ parameter, the fraction value must be between 0 and 1; an error is thrown if not. However, a null _`fraction`_ value simply produces a null result. 

**Table 9.62. Ordered-Set Aggregate Functions**

Function  Description | Partial Mode  
---|---  
`mode` () `WITHIN GROUP` ( `ORDER BY` `anyelement` ) → `anyelement` Computes the _mode_ , the most frequent value of the aggregated argument (arbitrarily choosing the first one if there are multiple equally-frequent values). The aggregated argument must be of a sortable type. | No  
`percentile_cont` ( _`fraction`_ `double precision` ) `WITHIN GROUP` ( `ORDER BY` `double precision` ) → `double precision` `percentile_cont` ( _`fraction`_ `double precision` ) `WITHIN GROUP` ( `ORDER BY` `interval` ) → `interval` Computes the _continuous percentile_ , a value corresponding to the specified _`fraction`_ within the ordered set of aggregated argument values. This will interpolate between adjacent input items if needed. | No  
`percentile_cont` ( _`fractions`_ `double precision[]` ) `WITHIN GROUP` ( `ORDER BY` `double precision` ) → `double precision[]` `percentile_cont` ( _`fractions`_ `double precision[]` ) `WITHIN GROUP` ( `ORDER BY` `interval` ) → `interval[]` Computes multiple continuous percentiles. The result is an array of the same dimensions as the _`fractions`_ parameter, with each non-null element replaced by the (possibly interpolated) value corresponding to that percentile. | No  
`percentile_disc` ( _`fraction`_ `double precision` ) `WITHIN GROUP` ( `ORDER BY` `anyelement` ) → `anyelement` Computes the _discrete percentile_ , the first value within the ordered set of aggregated argument values whose position in the ordering equals or exceeds the specified _`fraction`_. The aggregated argument must be of a sortable type. | No  
`percentile_disc` ( _`fractions`_ `double precision[]` ) `WITHIN GROUP` ( `ORDER BY` `anyelement` ) → `anyarray` Computes multiple discrete percentiles. The result is an array of the same dimensions as the _`fractions`_ parameter, with each non-null element replaced by the input value corresponding to that percentile. The aggregated argument must be of a sortable type. | No  
  
  


Each of the “hypothetical-set” aggregates listed in [Table 9.63](functions-aggregate.md#FUNCTIONS-HYPOTHETICAL-TABLE "Table 9.63. Hypothetical-Set Aggregate Functions") is associated with a window function of the same name defined in [Section 9.22](functions-window.md "9.22. Window Functions"). In each case, the aggregate's result is the value that the associated window function would have returned for the “hypothetical” row constructed from _`args`_ , if such a row had been added to the sorted group of rows represented by the _`sorted_args`_. For each of these functions, the list of direct arguments given in _`args`_ must match the number and types of the aggregated arguments given in _`sorted_args`_. Unlike most built-in aggregates, these aggregates are not strict, that is they do not drop input rows containing nulls. Null values sort according to the rule specified in the `ORDER BY` clause. 

**Table 9.63. Hypothetical-Set Aggregate Functions**

Function  Description | Partial Mode  
---|---  
`rank` ( _`args`_ ) `WITHIN GROUP` ( `ORDER BY` _`sorted_args`_ ) → `bigint` Computes the rank of the hypothetical row, with gaps; that is, the row number of the first row in its peer group. | No  
`dense_rank` ( _`args`_ ) `WITHIN GROUP` ( `ORDER BY` _`sorted_args`_ ) → `bigint` Computes the rank of the hypothetical row, without gaps; this function effectively counts peer groups. | No  
`percent_rank` ( _`args`_ ) `WITHIN GROUP` ( `ORDER BY` _`sorted_args`_ ) → `double precision` Computes the relative rank of the hypothetical row, that is (`rank` \- 1) / (total rows - 1). The value thus ranges from 0 to 1 inclusive. | No  
`cume_dist` ( _`args`_ ) `WITHIN GROUP` ( `ORDER BY` _`sorted_args`_ ) → `double precision` Computes the cumulative distribution, that is (number of rows preceding or peers with hypothetical row) / (total rows). The value thus ranges from 1/_`N`_ to 1. | No  
  
  


**Table 9.64. Grouping Operations**

Function  Description   
---  
`GROUPING` ( _`group_by_expression(s)`_ ) → `integer` Returns a bit mask indicating which `GROUP BY` expressions are not included in the current grouping set. Bits are assigned with the rightmost argument corresponding to the least-significant bit; each bit is 0 if the corresponding expression is included in the grouping criteria of the grouping set generating the current result row, and 1 if it is not included.   
  
  


The grouping operations shown in [Table 9.64](functions-aggregate.md#FUNCTIONS-GROUPING-TABLE "Table 9.64. Grouping Operations") are used in conjunction with grouping sets (see [Section 7.2.4](queries-table-expressions.md#QUERIES-GROUPING-SETS "7.2.4. GROUPING SETS, CUBE, and ROLLUP")) to distinguish result rows. The arguments to the `GROUPING` function are not actually evaluated, but they must exactly match expressions given in the `GROUP BY` clause of the associated query level. For example: 
    
    
    => **SELECT * FROM items_sold;**
     make  | model | sales
    -------+-------+-------
     Foo   | GT    |  10
     Foo   | Tour  |  20
     Bar   | City  |  15
     Bar   | Sport |  5
    (4 rows)
    
    => **SELECT make, model, GROUPING(make,model), sum(sales) FROM items_sold GROUP BY ROLLUP(make,model);**
     make  | model | grouping | sum
    -------+-------+----------+-----
     Foo   | GT    |        0 | 10
     Foo   | Tour  |        0 | 20
     Bar   | City  |        0 | 15
     Bar   | Sport |        0 | 5
     Foo   |       |        1 | 30
     Bar   |       |        1 | 20
           |       |        3 | 50
    (7 rows)
    

Here, the `grouping` value `0` in the first four rows shows that those have been grouped normally, over both the grouping columns. The value `1` indicates that `model` was not grouped by in the next-to-last two rows, and the value `3` indicates that neither `make` nor `model` was grouped by in the last row (which therefore is an aggregate over all the input rows). 

* * *

[Prev](functions-range.md "9.20. Range/Multirange Functions and Operators") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-window.md "9.22. Window Functions")  
---|---|---  
9.20. Range/Multirange Functions and Operators | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.22. Window Functions
