9.26. Set Returning Functions  
---  
[Prev](functions-comparisons.md "9.25. Row and Array Comparisons") | [Up](functions.md "Chapter 9. Functions and Operators")| Chapter 9. Functions and Operators| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](functions-info.md "9.27. System Information Functions and Operators")  
  
* * *

## 9.26. Set Returning Functions #

This section describes functions that possibly return more than one row. The most widely used functions in this class are series generating functions, as detailed in [Table 9.67](functions-srf.md#FUNCTIONS-SRF-SERIES "Table 9.67. Series Generating Functions") and [Table 9.68](functions-srf.md#FUNCTIONS-SRF-SUBSCRIPTS "Table 9.68. Subscript Generating Functions"). Other, more specialized set-returning functions are described elsewhere in this manual. See [Section 7.2.1.4](queries-table-expressions.md#QUERIES-TABLEFUNCTIONS "7.2.1.4. Table Functions") for ways to combine multiple set-returning functions. 

**Table 9.67. Series Generating Functions**

Function  Description   
---  
`generate_series` ( _`start`_ `integer`, _`stop`_ `integer` [, _`step`_ `integer` ] ) → `setof integer` `generate_series` ( _`start`_ `bigint`, _`stop`_ `bigint` [, _`step`_ `bigint` ] ) → `setof bigint` `generate_series` ( _`start`_ `numeric`, _`stop`_ `numeric` [, _`step`_ `numeric` ] ) → `setof numeric` Generates a series of values from _`start`_ to _`stop`_ , with a step size of _`step`_. _`step`_ defaults to 1.   
`generate_series` ( _`start`_ `timestamp`, _`stop`_ `timestamp`, _`step`_ `interval` ) → `setof timestamp` `generate_series` ( _`start`_ `timestamp with time zone`, _`stop`_ `timestamp with time zone`, _`step`_ `interval` [, _`timezone`_ `text` ] ) → `setof timestamp with time zone` Generates a series of values from _`start`_ to _`stop`_ , with a step size of _`step`_. In the timezone-aware form, times of day and daylight-savings adjustments are computed according to the time zone named by the _`timezone`_ argument, or the current [TimeZone](runtime-config-client.md#GUC-TIMEZONE) setting if that is omitted.   
  
  


When _`step`_ is positive, zero rows are returned if _`start`_ is greater than _`stop`_. Conversely, when _`step`_ is negative, zero rows are returned if _`start`_ is less than _`stop`_. Zero rows are also returned if any input is `NULL`. It is an error for _`step`_ to be zero. Some examples follow: 
    
    
    SELECT * FROM generate_series(2,4);
     generate_series
    -----------------
                   2
                   3
                   4
    (3 rows)
    
    SELECT * FROM generate_series(5,1,-2);
     generate_series
    -----------------
                   5
                   3
                   1
    (3 rows)
    
    SELECT * FROM generate_series(4,3);
     generate_series
    -----------------
    (0 rows)
    
    SELECT generate_series(1.1, 4, 1.3);
     generate_series
    -----------------
                 1.1
                 2.4
                 3.7
    (3 rows)
    
    -- this example relies on the date-plus-integer operator:
    SELECT current_date + s.a AS dates FROM generate_series(0,14,7) AS s(a);
       dates
    ------------
     2004-02-05
     2004-02-12
     2004-02-19
    (3 rows)
    
    SELECT * FROM generate_series('2008-03-01 00:00'::timestamp,
                                  '2008-03-04 12:00', '10 hours');
       generate_series
    ---------------------
     2008-03-01 00:00:00
     2008-03-01 10:00:00
     2008-03-01 20:00:00
     2008-03-02 06:00:00
     2008-03-02 16:00:00
     2008-03-03 02:00:00
     2008-03-03 12:00:00
     2008-03-03 22:00:00
     2008-03-04 08:00:00
    (9 rows)
    
    -- this example assumes that TimeZone is set to UTC; note the DST transition:
    SELECT * FROM generate_series('2001-10-22 00:00 -04:00'::timestamptz,
                                  '2001-11-01 00:00 -05:00'::timestamptz,
                                  '1 day'::interval, 'America/New_York');
        generate_series
    ------------------------
     2001-10-22 04:00:00+00
     2001-10-23 04:00:00+00
     2001-10-24 04:00:00+00
     2001-10-25 04:00:00+00
     2001-10-26 04:00:00+00
     2001-10-27 04:00:00+00
     2001-10-28 04:00:00+00
     2001-10-29 05:00:00+00
     2001-10-30 05:00:00+00
     2001-10-31 05:00:00+00
     2001-11-01 05:00:00+00
    (11 rows)
    

**Table 9.68. Subscript Generating Functions**

Function  Description   
---  
`generate_subscripts` ( _`array`_ `anyarray`, _`dim`_ `integer` ) → `setof integer` Generates a series comprising the valid subscripts of the _`dim`_ 'th dimension of the given array.   
`generate_subscripts` ( _`array`_ `anyarray`, _`dim`_ `integer`, _`reverse`_ `boolean` ) → `setof integer` Generates a series comprising the valid subscripts of the _`dim`_ 'th dimension of the given array. When _`reverse`_ is true, returns the series in reverse order.   
  
  


`generate_subscripts` is a convenience function that generates the set of valid subscripts for the specified dimension of the given array. Zero rows are returned for arrays that do not have the requested dimension, or if any input is `NULL`. Some examples follow: 
    
    
    -- basic usage:
    SELECT generate_subscripts('{NULL,1,NULL,2}'::int[], 1) AS s;
     s
    ---
     1
     2
     3
     4
    (4 rows)
    
    -- presenting an array, the subscript and the subscripted
    -- value requires a subquery:
    SELECT * FROM arrays;
             a
    --------------------
     {-1,-2}
     {100,200,300}
    (2 rows)
    
    SELECT a AS array, s AS subscript, a[s] AS value
    FROM (SELECT generate_subscripts(a, 1) AS s, a FROM arrays) foo;
         array     | subscript | value
    ---------------+-----------+-------
     {-1,-2}       |         1 |    -1
     {-1,-2}       |         2 |    -2
     {100,200,300} |         1 |   100
     {100,200,300} |         2 |   200
     {100,200,300} |         3 |   300
    (5 rows)
    
    -- unnest a 2D array:
    CREATE OR REPLACE FUNCTION unnest2(anyarray)
    RETURNS SETOF anyelement AS $$
    select $1[i][j]
       from generate_subscripts($1,1) g1(i),
            generate_subscripts($1,2) g2(j);
    $$ LANGUAGE sql IMMUTABLE;
    CREATE FUNCTION
    SELECT * FROM unnest2(ARRAY[[1,2],[3,4]]);
     unnest2
    ---------
           1
           2
           3
           4
    (4 rows)
    

When a function in the `FROM` clause is suffixed by `WITH ORDINALITY`, a `bigint` column is appended to the function's output column(s), which starts from 1 and increments by 1 for each row of the function's output. This is most useful in the case of set returning functions such as `unnest()`. 
    
    
    -- set returning function WITH ORDINALITY:
    SELECT * FROM pg_ls_dir('.') WITH ORDINALITY AS t(ls,n);
           ls        | n
    -----------------+----
     pg_serial       |  1
     pg_twophase     |  2
     postmaster.opts |  3
     pg_notify       |  4
     postgresql.conf |  5
     pg_tblspc       |  6
     logfile         |  7
     base            |  8
     postmaster.pid  |  9
     pg_ident.conf   | 10
     global          | 11
     pg_xact         | 12
     pg_snapshots    | 13
     pg_multixact    | 14
     PG_VERSION      | 15
     pg_wal          | 16
     pg_hba.conf     | 17
     pg_stat_tmp     | 18
     pg_subtrans     | 19
    (19 rows)
    

* * *

[Prev](functions-comparisons.md "9.25. Row and Array Comparisons") | [Up](functions.md "Chapter 9. Functions and Operators")|  [Next](functions-info.md "9.27. System Information Functions and Operators")  
---|---|---  
9.25. Row and Array Comparisons | [Home](index.md "PostgreSQL 17.5 Documentation")|  9.27. System Information Functions and Operators
