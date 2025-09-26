# TmFromChar

## Location
src/backend/utils/adt/formatting.c: 430 - 431

## Overview
A structure used to store intermediate parsing results when converting formatted date/time strings to internal timestamp representations in PostgreSQL's formatting system.

## Definition


## Detailed Description
TmFromChar serves as an intermediate parsing structure for PostgreSQL's date/time string parsing operations, primarily used by functions like  and . When parsing a formatted date/time string, various format elements are extracted and stored in this structure's fields before being converted to the final timestamp representation.

The structure accumulates parsed values from different format elements (year, month, day, hour, minute, second, timezone, etc.) and tracks parsing state including date mode (Gregorian vs ISO week), clock type (12/24 hour), and timezone information. After parsing completes, the accumulated values are validated and converted into PostgreSQL's internal timestamp format.

The structure is designed to handle incomplete date/time specifications - fields default to 0/NULL and missing components are handled appropriately during final timestamp construction.

## Parameters / Member Variables
- : Date parsing mode (FromCharDateMode) - tracks whether Gregorian or ISO week date elements are being used
- : Hour value (0-23 or 1-12 depending on clock mode)
- : AM/PM indicator (0=AM, 1=PM, used with 12-hour clock)
- : Minutes (0-59)
- Netid State   Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB   0      0                                               * 71924472              * 71924473         
u_str ESTAB   0      0                                               * 1998                  * 3655             
u_str ESTAB   0      0                                               * 76458145              * 76458144         
u_str ESTAB   0      0                                               * 26248598              * 26248599         
u_str ESTAB   0      0                                               * 26255694              * 26255695         
u_str ESTAB   0      0                                               * 26223452              * 26223453         
u_str ESTAB   0      0                                               * 26255689              * 26255688         
u_str ESTAB   0      0                                               * 26259857              * 26259856         
u_str ESTAB   0      0                                               * 33217459              * 33217458         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515568              * 15531228         
u_str ESTAB   0      0                                               * 10305                 * 10304            
u_str ESTAB   0      0                                               * 26255693              * 26255692         
u_str ESTAB   0      0                                               * 76458148              * 76458149         
u_str ESTAB   0      0                                               * 26259859              * 26259858         
u_str ESTAB   0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB   0      0                                               * 26248604              * 26248605         
u_str ESTAB   0      0                                               * 26223449              * 26223448         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB   0      0                                               * 71924474              * 71924475         
u_str ESTAB   0      0                                               * 26248605              * 26248604         
u_str ESTAB   0      0                                               * 33217456              * 33217457         
u_str ESTAB   0      0                                               * 71924471              * 71924470         
u_str ESTAB   0      0                                               * 76413901              * 76413900         
u_str ESTAB   0      0                                               * 26223448              * 26223449         
u_str ESTAB   0      0                                               * 26103565              * 26103564         
u_str ESTAB   0      0                                               * 26248603              * 26248602         
u_str ESTAB   0      0                                               * 26255688              * 26255689         
u_str ESTAB   0      0                                               * 26259862              * 26259863         
u_str ESTAB   0      0                                               * 26248602              * 26248603         
u_str ESTAB   0      0                                               * 33217457              * 33217456         
u_str ESTAB   0      0                                               * 15536473              * 15536472         
u_str ESTAB   0      0                                               * 26248600              * 26248601         
u_str ESTAB   0      0                                               * 7569                  * 1891             
u_str ESTAB   0      0                                               * 26255702              * 26255703         
u_str ESTAB   0      0                                               * 26255695              * 26255694         
u_str ESTAB   0      0                                               * 11302                 * 11303            
u_str ESTAB   0      0                                               * 9317                  * 9318             
u_str ESTAB   0      0                                               * 26259860              * 26259861         
u_str ESTAB   0      0                                               * 10304                 * 10305            
u_str ESTAB   0      0                                               * 14343                 * 0                
u_str ESTAB   0      0                                               * 71924475              * 71924474         
u_str ESTAB   0      0                                               * 71924473              * 71924472         
u_str ESTAB   0      0                                               * 55265971              * 55265970         
u_str ESTAB   0      0                                               * 26248601              * 26248600         
u_str ESTAB   0      0                                               * 33217461              * 33217460         
u_str ESTAB   0      0                                               * 76413900              * 76413901         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB   0      0                                               * 26259863              * 26259862         
u_str ESTAB   0      0                                               * 33217460              * 33217461         
u_str ESTAB   0      0                                               * 26248599              * 26248598         
u_str ESTAB   0      0                                               * 26103564              * 26103565         
u_str ESTAB   0      0                                               * 76458147              * 76458146         
u_str ESTAB   0      0                                               * 15536472              * 15536473         
u_str ESTAB   0      0                                               * 9318                  * 9317             
u_str ESTAB   0      0                                               * 71924470              * 71924471         
u_str ESTAB   0      0                                               * 26255703              * 26255702         
u_str ESTAB   0      0                                               * 76413898              * 76413899         
u_str ESTAB   0      0                                               * 26259858              * 26259859         
u_str ESTAB   0      0                                               * 55265970              * 55265971         
u_str ESTAB   0      0                                               * 33217458              * 33217459         
u_str ESTAB   0      0                                               * 26259861              * 26259860         
u_str ESTAB   0      0                                               * 26255690              * 26255691         
u_str ESTAB   0      0                                               * 26223453              * 26223452         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515571              * 15519569         
u_str ESTAB   0      0                                               * 3654                  * 10314            
u_str ESTAB   0      0                                               * 26259856              * 26259857         
u_str ESTAB   0      0                                               * 26255692              * 26255693         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB   0      0                                               * 55265972              * 55265973         
u_str ESTAB   0      0                                               * 3614                  * 3615             
u_str ESTAB   0      0                                               * 55265973              * 55265972         
u_str ESTAB   0      0                                               * 33217462              * 33217463         
u_str ESTAB   0      0                                               * 11303                 * 11302            
u_str ESTAB   0      0                                               * 11300                 * 11301            
u_str ESTAB   0      0                                               * 76458144              * 76458145         
u_str ESTAB   0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB   0      0                                               * 76413902              * 76413903         
u_str ESTAB   0      0                                               * 33217463              * 33217462         
u_str ESTAB   0      0                                               * 3615                  * 3614             
u_str ESTAB   0      0                                               * 76458149              * 76458148         
u_str ESTAB   0      0                                               * 26223450              * 26223451         
u_str ESTAB   0      0                                               * 76413903              * 76413902         
u_str ESTAB   0      0                                               * 76413899              * 76413898         
u_str ESTAB   0      0                                               * 11301                 * 11300            
u_str ESTAB   0      0                                               * 3606                  * 15372            
u_str ESTAB   0      0                                               * 15531228              * 15515568         
u_str ESTAB   0      0                                               * 26223451              * 26223450         
u_str ESTAB   0      0                                               * 15398                 * 4219             
u_str ESTAB   0      0                                               * 15519569              * 15515571         
u_str ESTAB   0      0                                               * 26255691              * 26255690         
u_str ESTAB   0      0                                               * 76458146              * 76458147         
tcp   ESTAB   0      0                                  172.30.249.175:41360     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:45879         127.0.0.1:42658            
tcp   ESTAB   0      0                                       127.0.0.1:59298         127.0.0.1:62628            
tcp   ESTAB   0      0                                       127.0.0.1:45879         127.0.0.1:42642            
tcp   ESTAB   0      0                                  172.30.249.175:41204     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:41194     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:42658         127.0.0.1:45879            
tcp   ESTAB   0      0                                       127.0.0.1:62628         127.0.0.1:59298            
tcp   ESTAB   0      0                                       127.0.0.1:42642         127.0.0.1:45879            
tcp   ESTAB   0      0                                  172.30.249.175:36606      104.16.29.34:https            
v_str ESTAB   0      0                                               *:633275402             2:50000            
v_str ESTAB   0      0                                               *:633275403             2:50000            
v_str ESTAB   0      0                                               *:633275404             2:50000            
v_str ESTAB   0      0                                               *:633275405             2:50000            
v_str ESTAB   0      0                                               *:633275406             2:50000            
v_str ESTAB   0      0                                               *:633275408             2:50001            
v_str ESTAB   0      0                                               *:633275409             2:50001            
v_str ESTAB   0      0                                               *:633275410             2:50001            
v_str ESTAB   0      0                                               *:633275424             2:50000            
v_str ESTAB   0      0                                               *:633275425             2:50000            
v_str ESTAB   0      0                                               *:633275426             2:50002            
v_str ESTAB   0      0                                               *:633275427             2:50002            
v_str ESTAB   0      0                                               *:633275428             2:50002            
v_str ESTAB   0      0                                               *:633275431             2:50002            
v_str ESTAB   0      0                                               *:633275432             2:50002            
v_str ESTAB   0      0                                               *:633275433             2:50002            
v_str ESTAB   0      0                                               *:1                     2:4102841729       
v_str ESTAB   0      0                                               *:633275822             2:390605012        
v_str ESTAB   0      0                                               *:633275823             2:390605017        
v_str ESTAB   0      0                                               *:633275823             2:390605016        
v_str ESTAB   0      0                                               *:633275823             2:390605015        
v_str ESTAB   0      0                                               *:633275823             2:390605014        
v_str ESTAB   0      0                                               *:633275823             2:390605013        
v_str ESTAB   0      0                                               *:633275854             2:390605314        
v_str ESTAB   0      0                                               *:633275855             2:390605319        
v_str ESTAB   0      0                                               *:633275855             2:390605318        
v_str ESTAB   0      0                                               *:633275855             2:390605317        
v_str ESTAB   0      0                                               *:633275855             2:390605316        
v_str ESTAB   0      0                                               *:633275855             2:390605315        
v_str ESTAB   0      0                                               *:633275852             2:390605303        
v_str ESTAB   0      0                                               *:633275853             2:390605308        
v_str ESTAB   0      0                                               *:633275853             2:390605307        
v_str ESTAB   0      0                                               *:633275853             2:390605306        
v_str ESTAB   0      0                                               *:633275853             2:390605305        
v_str ESTAB   0      0                                               *:633275853             2:390605304        
v_str ESTAB   0      0                                               *:633275411             2:4102841364       
v_str ESTAB   0      0                                               *:633275430             2:4102841703       
v_str ESTAB   0      0                                               *:633275430             2:4102841702       
v_str ESTAB   0      0                                               *:633275430             2:4102841701       
v_str CLOSING 0      0                                               *:633275430             2:4102841700       
v_str ESTAB   0      0                                               *:633275429             2:4102841697       
v_str ESTAB   0      0                                               *:633275435             2:4102841707       
v_str ESTAB   0      0                                               *:633275458             2:4102841830       
v_str ESTAB   0      0                                               *:633275458             2:4102841829       
v_str CLOSING 0      0                                               *:633275458             2:4102841828       
v_str CLOSING 0      0                                               *:633275458             2:4102841827       
v_str ESTAB   0      0                                               *:633275458             2:4102841826       
v_str ESTAB   0      0                                               *:633275457             2:4102841825       
v_str ESTAB   0      0                                               *:633275462             2:4102842074       
v_str ESTAB   0      0                                               *:633275462             2:4102842073       
v_str ESTAB   0      0                                               *:633275462             2:4102842072       
v_str ESTAB   0      0                                               *:633275462             2:4102842071       
v_str ESTAB   0      0                                               *:633275462             2:4102842070       
v_str ESTAB   0      0                                               *:633275463             2:4102842086       
v_str ESTAB   0      0                                               *:633275461             2:4102842069       
v_str ESTAB   0      0                                               *:633275466             2:4102842133       
v_str ESTAB   0      0                                               *:633275466             2:4102842132       
v_str ESTAB   0      0                                               *:633275466             2:4102842131       
v_str ESTAB   0      0                                               *:633275466             2:4102842130       
v_str ESTAB   0      0                                               *:633275466             2:4102842129       
v_str ESTAB   0      0                                               *:633275467             2:4102842156       
v_str ESTAB   0      0                                               *:633275464             2:4102842091       
v_str ESTAB   0      0                                               *:633275464             2:4102842090       
v_str ESTAB   0      0                                               *:633275464             2:4102842089       
v_str ESTAB   0      0                                               *:633275464             2:4102842088       
v_str ESTAB   0      0                                               *:633275464             2:4102842087       
v_str ESTAB   0      0                                               *:633275465             2:4102842128       
v_str ESTAB   0      0                                               *:633275470             2:4102842564       
v_str ESTAB   0      0                                               *:633275470             2:4102842563       
v_str ESTAB   0      0                                               *:633275470             2:4102842562       
v_str ESTAB   0      0                                               *:633275470             2:4102842561       
v_str ESTAB   0      0                                               *:633275470             2:4102842560       
v_str ESTAB   0      0                                               *:633275471             2:4102843465       
v_str ESTAB   0      0                                               *:633275468             2:4102842161       
v_str ESTAB   0      0                                               *:633275468             2:4102842160       
v_str ESTAB   0      0                                               *:633275468             2:4102842159       
v_str ESTAB   0      0                                               *:633275468             2:4102842158       
v_str ESTAB   0      0                                               *:633275468             2:4102842157       
v_str ESTAB   0      0                                               *:633275469             2:4102842559       
v_str ESTAB   0      0                                               *:633275472             2:4102843470       
v_str ESTAB   0      0                                               *:633275472             2:4102843469       
v_str CLOSING 0      0                                               *:633275472             2:4102843468       
v_str CLOSING 0      0                                               *:633275472             2:4102843467       
v_str ESTAB   0      0                                               *:633275472             2:4102843466       : Seconds (0-59)  
- : Seconds since midnight (alternative seconds representation)
- : Day of week (1-7, Sunday=1, 0=missing)
- : Day of month (1-31)
- : Day of year (1-366)
- : Month (1-12)
- : Milliseconds (0-999)
- : Year value
- : BC/AD indicator (1=BC, 0=AD)
- : Week of year 
-  19:57:40 up 10 days, 12:23,  0 users,  load average: 0.58, 0.54, 0.54
USER     TTY      FROM             LOGIN@   IDLE   JCPU   PCPU WHAT: Week of month
- : Century
- : Julian day number
- : Microseconds (0-999999)
- : Year size indicator (2=YY format, 4=YYYY format)
- : Clock format (CLOCK_12_HOUR=1 or CLOCK_24_HOUR=0)
- : Timezone sign (+1, -1, or 0 if no TZH/TZM fields)
- : Timezone hours offset
- : Timezone minutes offset
- : Fractional seconds precision
- : Boolean indicating presence of timezone field in input
- : GMT offset in seconds for fixed-offset timezone abbreviations
- : Pointer to timezone structure for dynamic timezone abbreviations
- : String containing dynamic timezone abbreviation

## Dependencies
- Functions called/Symbols referenced:
  - FromCharDateMode enum (for mode tracking)
  - pg_tz structure (for timezone handling)
  - ZERO_tmfc macro (for initialization)
- Called from (representative examples):
  - DCH_from_char: Main parsing function that populates TmFromChar
  - do_to_timestamp: Uses TmFromChar for timestamp conversion
  - from_char_set_mode: Validates and sets parsing mode

## Notes and Other Information
- Initialized using the ZERO_tmfc macro which zeroes all fields using memset
- Field values of 0 typically indicate missing or unspecified components
- The structure supports both Gregorian calendar and ISO 8601 week date parsing modes
- Timezone handling supports both fixed offsets (via gmtoffset) and dynamic zones (via tzp/abbrev)
- Year handling includes special logic for 2-digit vs 4-digit years (yysz field)
- After parsing, values are validated and converted to PostgreSQL's internal timestamp representation in struct pg_tm
- The structure is designed to be stack-allocated and temporary - used only during parsing operations