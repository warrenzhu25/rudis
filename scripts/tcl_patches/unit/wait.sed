# Applied by scripts/run_redis_integration_suite.sh to its copy of
# unit/wait.tcl.
#
# "WAITAOF local client unblock with timeout and error" turns AOF on, which
# starts a rewrite, then expects WAITAOF 1 0 0 to stay blocked (appendfsync
# no: nothing fsyncs) until CLIENT UNBLOCK ends it. rudis rewrites the AOF
# shard by shard, each shard switching to its new, fsynced file as it
# finishes, so a write made during the rewrite really is fsynced once its
# shard is done, and WAITAOF counts it; Redis counts only what was there
# when AOF was turned on until the whole rewrite ends. On a slow host the
# client was released before CLIENT UNBLOCK. Wait for the rewrite to end,
# after which rudis, like Redis, leaves the write unsynced.
/test {WAITAOF local client unblock with timeout and error} {/,/set rd \[redis_deferring_client\]/{
    /r config set appendfsync no/a\
            waitForBgrewriteaof r
}
