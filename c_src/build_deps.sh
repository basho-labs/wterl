#!/bin/bash

set -e

WT_BRANCH=basho
WT_REMOTE_REPO=http://github.com/wiredtiger/wiredtiger.git

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
                git merge origin/${WT_BRANCH})
        else
            git clone -b ${WT_BRANCH} --single-branch ${WT_REMOTE_REPO} && \
            (cd wiredtiger && \
             patch -p1 < ../wiredtiger-extension-link.patch && \
             ./autogen.sh)
        fi
        (cd wiredtiger/build_posix && \
            CFLAGS="-I/usr/local/include -L/usr/local/lib" \
	    ../configure --with-pic \
                         --enable-snappy \
                         --prefix=${BASEDIR}/system && \
            make -j && make install)
        [ -d ${BASEDIR}/../priv ] || mkdir ${BASEDIR}/../priv
        cp ${BASEDIR}/system/bin/wt ${BASEDIR}/../priv
        cp ${BASEDIR}/system/lib/*.so ${BASEDIR}/../priv
        ;;
esac
