# Applied by scripts/run_redis_integration_suite.sh to its copy of
# integration/corrupt-dump.tcl.
#
# Rudis validates every RESTORE payload in full, always (as Redis does with
# sanitize-dump-payload yes), and refuses a corrupt one with "Bad data
# format", logging "DUMP payload integrity check failed: <reason>". Many of
# these tests run Redis with sanitize-dump-payload no, where it accepts the
# payload and then exits on an assertion when the value is used. For rudis
# they check instead that the payload was refused, logged, and that the
# server is still up.
#
# 1. A RESTORE Redis accepts without sanitization may fail: wrap it in a
#    catch, unless it already is the body of one (the previous line, kept
#    in the hold space, opens it).
/^\s*r restore /I{
x
/catch {\s*$/{
x
b keep
}
x
s/^\(\s*\)r restore \(.*\)$/\1catch {r restore \2}/I
:keep
}
h
# 2. "Redis crashed on an assertion" becomes "refused, logged, still up".
s/assert_equal \[count_log_message 0 "\(ASSERTION FAILED\|Guru Meditation\|listpack with dup elements\)"\] 1/assert_equal [r ping] PONG; verify_log_message 0 "*integrity check failed*" 0/
# 3. Redis's per-check log messages: rudis logs its own reason after the
#    common "integrity check failed".
s/verify_log_message 0 "\*\(Stream listpack\|Zset ziplist\|Zipmap\) integrity check failed\*" 0/verify_log_message 0 "*integrity check failed*" 0/
s/verify_log_message 0 "\*\(Duplicate stream consumer detected\|Hash zipmap with dup elements\|Stream cgroup entries_read inconsistent with entries_added\|Stream consumer PEL entry already has a consumer assigned\|Stream entries_added inconsistent with length\)\*" 0/verify_log_message 0 "*integrity check failed*" 0/
