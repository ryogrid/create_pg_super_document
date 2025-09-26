# TapeShare

## Location
[src/include/utils/logtape.h:48-55](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/logtape.h#L48-L55)

## Overview
TapeShare is a metadata structure used in PostgreSQLs parallel external sorting to share information about materialized tape data between worker processes and the leader process during parallel tuplesort operations.