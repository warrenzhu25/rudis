# Applied by scripts/run_redis_integration_suite.sh to its copy of
# unit/cluster/atomic-slot-migration.tcl.
#
# "Eviction does not evict keys in importing slots" leaves node 1 with
# maxmemory set to 200 KB above its usage at the start, and the later tests
# write to node 1. Once the test's keys are evicted, rudis's node is only
# ~250 KB under the limit with nothing left to evict (measured), so memory
# that isn't keys growing during a later pipelined populate (connection
# and replication buffers, say) sometimes tips it over and writes get
# -OOM. Redis leaves replica output and AOF buffers out of the eviction
# count (mem_not_counted_for_evict); rudis counts everything. Reset the
# limit after that test so the later tests don't depend on that margin.
/assert_equal {} \[scan \[regexp -inline {expires/a\
        R 1 config set maxmemory 0
