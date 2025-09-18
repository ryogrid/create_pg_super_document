# WindowAggState

## Location
src/include/nodes/execnodes.h: 2559 - 2643

## Overview
WindowAggState is the primary execution state node for PostgreSQL's window function operations, managing the complete lifecycle of window function computation including frame management, partitioning, buffering, and aggregate evaluation.

## Definition


## Detailed Description
WindowAggState is the comprehensive execution state structure for window function operations in PostgreSQL. It inherits from ScanState and manages all aspects of window function execution including partitioning data, maintaining window frames, buffering tuples, and evaluating both regular window functions and aggregate functions used in window contexts. The structure supports various frame types (ROWS, RANGE, GROUPS), handles complex frame boundary specifications, manages memory contexts for different computation phases, and provides optimization features like run conditions for early termination and pass-through modes for performance.

## Parameters / Member Variables
- Netid State   Recv-Q Send-Q                              Local Address:Port       Peer Address:Port      Process
u_str ESTAB   0      0                                               * 16331009              * 16331008         
u_str ESTAB   0      0                                               * 18065001              * 18065000         
u_str ESTAB   0      0                                               * 16883729              * 16883728         
u_str ESTAB   0      0                                               * 16876188              * 16876189         
u_str ESTAB   0      0                                               * 1998                  * 3655             
u_str ESTAB   0      0                                               * 16331008              * 16331009         
u_str ESTAB   0      0                                               * 16323072              * 16323071         
u_str ESTAB   0      0                                               * 16930623              * 16930624         
u_str ESTAB   0      0                                               * 16948454              * 16948455         
u_str ESTAB   0      0                                               * 16318726              * 16318727         
u_str ESTAB   0      0                                               * 18061758              * 18061757         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515568              * 15531228         
u_str ESTAB   0      0                                               * 16876182              * 16876183         
u_str ESTAB   0      0                                               * 10305                 * 10304            
u_str ESTAB   0      0                                               * 16321329              * 16321330         
u_str ESTAB   0      0                                               * 16318723              * 16318722         
u_str ESTAB   0      0                                               * 18061762              * 18061761         
u_str ESTAB   0      0                                               * 16948457              * 16948456         
u_str ESTAB   0      0                                               * 16876189              * 16876188         
u_str ESTAB   0      0                            /tmp/dbus-vEvJ09Fzqf 10314                 * 3654             
u_str ESTAB   0      0                                               * 16321327              * 16321328         
u_str ESTAB   0      0                                               * 20093174              * 20093173         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15372                 * 3606             
u_str ESTAB   0      0                                               * 16321326              * 16321325         
u_str ESTAB   0      0                                               * 16948458              * 16948459         
u_str ESTAB   0      0                                               * 16876185              * 16876184         
u_str ESTAB   0      0                                               * 18065005              * 18065004         
u_str ESTAB   0      0                                               * 16323069              * 16323070         
u_str ESTAB   0      0                                               * 20093170              * 20093169         
u_str ESTAB   0      0                                               * 18061759              * 18061760         
u_str ESTAB   0      0                                               * 16883731              * 16883730         
u_str ESTAB   0      0                                               * 18065007              * 18065006         
u_str ESTAB   0      0                                               * 16331007              * 16331006         
u_str ESTAB   0      0                                               * 16318725              * 16318724         
u_str ESTAB   0      0                                               * 16930624              * 16930623         
u_str ESTAB   0      0                                               * 16930619              * 16930620         
u_str ESTAB   0      0                                               * 16948461              * 16948460         
u_str ESTAB   0      0                                               * 16876184              * 16876185         
u_str ESTAB   0      0                                               * 18061764              * 18061763         
u_str ESTAB   0      0                                               * 18065000              * 18065001         
u_str ESTAB   0      0                                               * 16883735              * 16883734         
u_str ESTAB   0      0                                               * 15536473              * 15536472         
u_str ESTAB   0      0                                               * 16948455              * 16948454         
u_str ESTAB   0      0                                               * 7569                  * 1891             
u_str ESTAB   0      0                                               * 11302                 * 11303            
u_str ESTAB   0      0                                               * 16883730              * 16883731         
u_str ESTAB   0      0                                               * 9317                  * 9318             
u_str ESTAB   0      0                                               * 16323070              * 16323069         
u_str ESTAB   0      0                                               * 10304                 * 10305            
u_str ESTAB   0      0                                               * 14343                 * 0                
u_str ESTAB   0      0                                               * 16876183              * 16876182         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 1891                  * 7569             
u_str ESTAB   0      0                                               * 16323076              * 16323075         
u_str ESTAB   0      0                                               * 16330961              * 16330960         
u_str ESTAB   0      0                                               * 16883733              * 16883732         
u_str ESTAB   0      0                                               * 16323073              * 16323074         
u_str ESTAB   0      0                                               * 16331006              * 16331007         
u_str ESTAB   0      0                                               * 16883732              * 16883733         
u_str ESTAB   0      0                                               * 20093172              * 20093171         
u_str ESTAB   0      0                                               * 15536472              * 15536473         
u_str ESTAB   0      0                                               * 9318                  * 9317             
u_str ESTAB   0      0                                               * 16930620              * 16930619         
u_str ESTAB   0      0                                               * 16323075              * 16323076         
u_str ESTAB   0      0                                               * 16331002              * 16331003         
u_str ESTAB   0      0                                               * 16948456              * 16948457         
u_str ESTAB   0      0                                               * 16331004              * 16331005         
u_str ESTAB   0      0                                               * 18065002              * 18065003         
u_str ESTAB   0      0                                               * 16930622              * 16930621         
u_str ESTAB   0      0                                               * 20093173              * 20093174         
u_str ESTAB   0      0                                               * 16318724              * 16318725         
u_str ESTAB   0      0                               /tmp/.X11-unix/X0 15515571              * 15519569         
u_str ESTAB   0      0                                               * 16330957              * 16330958         
u_str ESTAB   0      0                                               * 3654                  * 10314            
u_str ESTAB   0      0                                               * 16321323              * 16321324         
u_str ESTAB   0      0                                               * 18061757              * 18061758         
u_str ESTAB   0      0                                               * 16323071              * 16323072         
u_str ESTAB   0      0                                               * 16330960              * 16330961         
u_str ESTAB   0      0                                               * 16321330              * 16321329         
u_str ESTAB   0      0      /var/run/docker/containerd/containerd.sock 4219                  * 15398            
u_str ESTAB   0      0                                               * 3614                  * 3615             
u_str ESTAB   0      0                                               * 16930621              * 16930622         
u_str ESTAB   0      0                                               * 16930626              * 16930625         
u_str ESTAB   0      0                                               * 16948459              * 16948458         
u_str ESTAB   0      0                                               * 16876186              * 16876187         
u_str ESTAB   0      0                                               * 20093169              * 20093170         
u_str ESTAB   0      0                                               * 18061763              * 18061764         
u_str ESTAB   0      0                                               * 18065004              * 18065005         
u_str ESTAB   0      0                                               * 16883728              * 16883729         
u_str ESTAB   0      0                                               * 16321325              * 16321326         
u_str ESTAB   0      0                                               * 11303                 * 11302            
u_str ESTAB   0      0                                               * 11300                 * 11301            
u_str ESTAB   0      0                                               * 16323074              * 16323073         
u_str ESTAB   0      0                                               * 16330958              * 16330957         
u_str ESTAB   0      0                                               * 16318722              * 16318723         
u_str ESTAB   0      0                     /mnt/wslg/PulseAudioRDPSink 3655                  * 1998             
u_str ESTAB   0      0                                               * 16876187              * 16876186         
u_str ESTAB   0      0                                               * 16883734              * 16883735         
u_str ESTAB   0      0                                               * 3615                  * 3614             
u_str ESTAB   0      0                                               * 16948460              * 16948461         
u_str ESTAB   0      0                                               * 16331005              * 16331004         
u_str ESTAB   0      0                                               * 16930625              * 16930626         
u_str ESTAB   0      0                                               * 18061761              * 18061762         
u_str ESTAB   0      0                                               * 11301                 * 11300            
u_str ESTAB   0      0                                               * 3606                  * 15372            
u_str ESTAB   0      0                                               * 15531228              * 15515568         
u_str ESTAB   0      0                                               * 18065006              * 18065007         
u_str ESTAB   0      0                                               * 16321328              * 16321327         
u_str ESTAB   0      0                                               * 15398                 * 4219             
u_str ESTAB   0      0                                               * 16318727              * 16318726         
u_str ESTAB   0      0                                               * 18061760              * 18061759         
u_str ESTAB   0      0                                               * 15519569              * 15515571         
u_str ESTAB   0      0                                               * 20093171              * 20093172         
u_str ESTAB   0      0                                               * 16321324              * 16321323         
u_str ESTAB   0      0                                               * 18065003              * 18065002         
u_str ESTAB   0      0                                               * 16331003              * 16331002         
tcp   ESTAB   0      0                                       127.0.0.1:37962         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:59952         127.0.0.1:37353            
tcp   ESTAB   0      459                                172.30.249.175:3400       172.30.240.1:60802            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37962            
tcp   ESTAB   0      0                                       127.0.0.1:59958         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:48262         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48268            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:37970            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:48262            
tcp   ESTAB   0      0                                  172.30.249.175:48734     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59958            
tcp   ESTAB   0      0                                       127.0.0.1:48268         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37970         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:45894         127.0.0.1:37353            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45894            
tcp   ESTAB   0      0                                       127.0.0.1:45882         127.0.0.1:37353            
tcp   ESTAB   0      0                                  172.30.249.175:38696     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:38698     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:59090     160.79.104.10:https            
tcp   ESTAB   0      0                                  172.30.249.175:48724     160.79.104.10:https            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:45882            
tcp   ESTAB   0      0                                       127.0.0.1:37353         127.0.0.1:59952            
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
v_str ESTAB   0      0                                               *:633275411             2:4102841364       
v_str ESTAB   0      0                                               *:633275671             2:342791897        
v_str ESTAB   0      0                                               *:633275674             2:342791913        
v_str ESTAB   0      0                                               *:633275674             2:342791912        
v_str ESTAB   0      0                                               *:633275674             2:342791911        
v_str ESTAB   0      0                                               *:633275674             2:342791910        
v_str ESTAB   0      0                                               *:633275674             2:342791909        
v_str ESTAB   0      0                                               *:633275675             2:342791919        
v_str ESTAB   0      0                                               *:633275672             2:342791902        
v_str ESTAB   0      0                                               *:633275672             2:342791901        
v_str ESTAB   0      0                                               *:633275672             2:342791900        
v_str ESTAB   0      0                                               *:633275672             2:342791899        
v_str ESTAB   0      0                                               *:633275672             2:342791898        
v_str ESTAB   0      0                                               *:633275673             2:342791908        
v_str ESTAB   0      0                                               *:633275676             2:342791924        
v_str ESTAB   0      0                                               *:633275676             2:342791923        
v_str ESTAB   0      0                                               *:633275676             2:342791922        
v_str ESTAB   0      0                                               *:633275676             2:342791921        
v_str ESTAB   0      0                                               *:633275676             2:342791920        
v_str ESTAB   0      0                                               *:633275430             2:4102841703       
v_str ESTAB   0      0                                               *:633275430             2:4102841702       
v_str ESTAB   0      0                                               *:633275430             2:4102841701       
v_str CLOSING 0      0                                               *:633275430             2:4102841700       
v_str ESTAB   0      0                                               *:633275429             2:4102841697       
v_str ESTAB   0      0                                               *:633275435             2:4102841707       
v_str ESTAB   0      0                                               *:633275691             2:342792670        
v_str ESTAB   0      0                                               *:633275694             2:342792686        
v_str ESTAB   0      0                                               *:633275694             2:342792685        
v_str ESTAB   0      0                                               *:633275694             2:342792684        
v_str ESTAB   0      0                                               *:633275694             2:342792683        
v_str ESTAB   0      0                                               *:633275694             2:342792682        
v_str ESTAB   0      0                                               *:633275692             2:342792675        
v_str ESTAB   0      0                                               *:633275692             2:342792674        
v_str ESTAB   0      0                                               *:633275692             2:342792673        
v_str ESTAB   0      0                                               *:633275692             2:342792672        
v_str ESTAB   0      0                                               *:633275692             2:342792671        
v_str ESTAB   0      0                                               *:633275693             2:342792681        
v_str ESTAB   0      0                                               *:633275703             2:342792823        
v_str ESTAB   0      0                                               *:633275706             2:342792839        
v_str ESTAB   0      0                                               *:633275706             2:342792838        
v_str ESTAB   0      0                                               *:633275706             2:342792837        
v_str ESTAB   0      0                                               *:633275706             2:342792836        
v_str ESTAB   0      0                                               *:633275706             2:342792835        
v_str ESTAB   0      0                                               *:633275704             2:342792828        
v_str ESTAB   0      0                                               *:633275704             2:342792827        
v_str ESTAB   0      0                                               *:633275704             2:342792826        
v_str ESTAB   0      0                                               *:633275704             2:342792825        
v_str ESTAB   0      0                                               *:633275704             2:342792824        
v_str ESTAB   0      0                                               *:633275705             2:342792834        
v_str ESTAB   0      0                                               *:633275458             2:4102841830       
v_str ESTAB   0      0                                               *:633275458             2:4102841829       
v_str CLOSING 0      0                                               *:633275458             2:4102841828       
v_str CLOSING 0      0                                               *:633275458             2:4102841827       
v_str ESTAB   0      0                                               *:633275458             2:4102841826       
v_str ESTAB   0      0                                               *:633275711             2:342793593        
v_str ESTAB   0      0                                               *:633275457             2:4102841825       
v_str ESTAB   0      0                                               *:633275462             2:4102842074       
v_str ESTAB   0      0                                               *:633275462             2:4102842073       
v_str ESTAB   0      0                                               *:633275462             2:4102842072       
v_str ESTAB   0      0                                               *:633275462             2:4102842071       
v_str ESTAB   0      0                                               *:633275462             2:4102842070       
v_str ESTAB   0      0                                               *:633275714             2:342793609        
v_str ESTAB   0      0                                               *:633275714             2:342793608        
v_str ESTAB   0      0                                               *:633275714             2:342793607        
v_str ESTAB   0      0                                               *:633275714             2:342793606        
v_str ESTAB   0      0                                               *:633275714             2:342793605        
v_str ESTAB   0      0                                               *:633275463             2:4102842086       
v_str ESTAB   0      0                                               *:633275712             2:342793598        
v_str ESTAB   0      0                                               *:633275712             2:342793597        
v_str ESTAB   0      0                                               *:633275712             2:342793596        
v_str ESTAB   0      0                                               *:633275712             2:342793595        
v_str ESTAB   0      0                                               *:633275712             2:342793594        
v_str ESTAB   0      0                                               *:633275461             2:4102842069       
v_str ESTAB   0      0                                               *:633275713             2:342793604        
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
v_str ESTAB   0      0                                               *:633275472             2:4102843468       
v_str ESTAB   0      0                                               *:633275472             2:4102843467       
v_str ESTAB   0      0                                               *:633275472             2:4102843466       : Base ScanState structure containing common execution node fields
- : List of all WindowFunc nodes present in the target list
- : Total count of window functions to be evaluated
- : Count of plain aggregate functions used as window functions
- : Array of per-window-function state information
- : Array of per-aggregate state information for plain aggregates
- : ExprState for equality functions used in partition comparisons
- : ExprState for equality functions used in ordering comparisons
- : Tuplestore containing all rows of the current partition
- : Read pointer number for the currently processing row
- : Read pointer number for the current frame's head position
- : Read pointer number for the current frame's tail position
- : Read pointer number for the current peer group's tail position
- : Total number of rows currently stored in the buffer
- : Position of current row within the current partition (0-based)
- : Position of the current frame's head row
- : Position one past the current frame's tail row
- : WindowObject used for aggregate function fetches
- : Starting row position for current aggregate computations
- : All rows before this position have been aggregated
- : Current execution status of the WindowAgg node
- : Frame clause options specifying frame type and bounds
- : ExprState for evaluating frame start boundary offset
- : ExprState for evaluating frame end boundary offset
- : Computed value of the start offset expression
- : Computed value of the end offset expression
- : FmgrInfo for in_range function used with start offset
- : FmgrInfo for in_range function used with end offset
- : Collation OID for in_range function comparisons
- : Boolean indicating ascending sort order for in_range tests
- : Boolean indicating null ordering for in_range tests
- : Peer group number of the current row in partition
- : Peer group number of the frame head row
- : Peer group number of the frame tail row
- : Position of current row's peer group head
- : Position one past current row's peer group tail
- : Memory context for partition-lifetime data
- : Shared memory context for aggregate working data
- : Memory context for currently active aggregate
- : ExprContext for short-term expression evaluation
- : ExprState for condition that controls continued execution
- : Flag controlling behavior when run condition fails
- : Flag indicating if this is the topmost WindowAgg node
- : Flag indicating if this is the start of scanning
- : Flag indicating if current partition is fully buffered
- : Flag indicating if more partitions exist to process
- : Flag indicating if frame head position is current
- : Flag indicating if frame tail position is current
- : Flag indicating if group tail position is current
- : TupleTableSlot holding first tuple of current/next partition
- : TupleTableSlot holding first tuple of current frame
- : TupleTableSlot holding first tuple after current frame
- : Temporary TupleTableSlot for aggregate computations
- : General purpose temporary TupleTableSlot
- : General purpose temporary TupleTableSlot

## Dependencies
- Functions called/Symbols referenced:
  - ScanState
  - WindowStatePerFunc
  - WindowStatePerAgg
  - WindowObjectData
  - WindowAggStatus
  - Tuplestorestate
  - ExprState
  - FmgrInfo
  - MemoryContext
  - TupleTableSlot
- Called from (representative examples):
  - ExecWindowAgg
  - ExecInitWindowAgg
  - ExecEndWindowAgg
  - ExecReScanWindowAgg
  - begin_partition
  - eval_windowaggregates
  - update_frameheadpos
  - update_frametailpos

## Notes and Other Information
WindowAggState is one of the most sophisticated execution nodes in PostgreSQL, supporting the full complexity of SQL window functions including multiple frame types (ROWS, RANGE, GROUPS), peer group management, and various optimization strategies. The structure's design enables efficient processing of window functions through intelligent buffering, frame boundary caching, and support for both incremental and batch computation modes. The run condition mechanism allows for query optimization scenarios where window function evaluation can be terminated early or switched to pass-through mode when certain conditions are no longer met.