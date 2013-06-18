#!/bin/sh -

# Note: also, remember to update version numbers in rpath specs so that shared libs can be found at runtime!!!

wterl=`git describe --always --long --tags`
wiredtiger0=`(cd c_src/wiredtiger-[0-9.]* && git describe --always --long --tags)`
wiredtiger=`echo $wiredtiger0 | awk '{print $2}'`

echo $wterl
echo $wiredtiger
