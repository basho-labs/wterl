#!/bin/bash

set -e

WT_VSN=1.3.7

[ `basename $PWD` != "c_src" ] && cd c_src

BASEDIR="$PWD"

case "$1" in
    clean)
        rm -rf system wiredtiger-$WT_VSN
        ;;

    *)
        test -f system/lib/libwiredtiger.a && exit 0

        tar -xjf wiredtiger-$WT_VSN.tar.bz2

        (cd wiredtiger-$WT_VSN/build_posix && \
            ../configure --with-pic \
                         --prefix=$BASEDIR/system && \
            make && make install)

        ;;
esac

