# Applied by scripts/run_redis_integration_suite.sh to its copy of
# unit/pubsub.tcl.
#
# "... succeeds with small channel but fails with large ..." starts right
# after the previous OOM tests closed their clients, each holding megabytes
# (a 2 MB channel name in its query buffer). rudis frees a closed client's
# memory on the core that served it, which may not have run yet when the
# next command arrives on another core (Redis's single thread always
# handles the close first), so the first small SUBSCRIBE under the 5 MB
# limit was sometimes refused. Wait for those clients to be gone.
/proc test_subscribe_small_then_large_oom {cmd channel_type} {/,/r config set maxmemory-policy noeviction/{
    /r config set maxmemory-policy noeviction/a\
            wait_for_condition 100 50 {[s connected_clients] == 1} else {fail "earlier clients still connected"}
}
