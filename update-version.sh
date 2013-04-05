#!/bin/sh -

wterl=`git log -n 1 --pretty=format:"%H"`
wiredtiger0=`(cd c_src/wiredtiger && git log -n 1 --pretty=format:"%H")`
wiredtiger=`echo $wiredtiger0 | awk '{print $2}'`

echo $wterl
echo $wiredtiger

