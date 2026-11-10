# Applied by scripts/run_redis_integration_suite.sh to its copy of
# integration/replication-3.tcl.
#
# "Writable replica doesn't return expired keys" (skipped for rudis, see
# scripts/tcl_skips/integration/replication-3.txt) is the test that sets the
# replica back to slave-read-only yes, after "Slave is able to evict keys
# created in writable slaves" left it writable. With it skipped, the PFCOUNT
# tests that follow would run on a writable replica, where rudis deletes an
# expired key on reads. Set the replica read-only where they start.
/test {PFCOUNT updates cache on readonly replica} {/a\
            r -1 config set slave-read-only yes
