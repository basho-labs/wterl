#!/bin/bash

set -e

WT_BRANCH=basho

[ `basename $PWD` != "c_src" ] && cd c_src

BASEDIR="$PWD"

case "$1" in
    clean)
        rm -rf system wiredtiger
        ;;

    *)
        test -f system/lib/libwiredtiger.a && exit 0

        if [ -d wiredtiger/.git ]; then
            (cd wiredtiger && \
                git fetch && \
                git merge origin/$WT_BRANCH)
        else
            git clone http://github.com/wiredtiger/wiredtiger.git -b $WT_BRANCH && \
                (cd wiredtiger && ./autogen.sh)
        fi
        (cd wiredtiger/build_posix && \
            ../configure --with-pic \
                         --enable-snappy \
                         --enable-bzip2 \
                         --prefix=$BASEDIR/system && \
            make -j && make install)
	cp system/bin/wt ../priv
	cp system/lib/*.so ../priv
        ;;
esac
