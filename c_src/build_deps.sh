#!/bin/bash

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

set -e

WT_REPO=http://github.com/wiredtiger/wiredtiger.git
WT_BRANCH=basho
WT_VSN=""
WT_DIR=wiredtiger-$WT_BRANCH

SNAPPY_VSN="1.0.4"
SNAPPY_DIR=snappy-$SNAPPY_VSN

BZIP2_VSN="1.0.6"
BZIP2_DIR=bzip2-$BZIP2_VSN

[ `basename $PWD` != "c_src" ] && cd c_src

export BASEDIR="$PWD"

which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$BASEDIR/system/lib:$LD_LIBRARY_PATH"

get_wt ()
{
    if [ -d $BASEDIR/$WT_DIR/.git ]; then
        (cd $BASEDIR/$WT_DIR && git pull -u) || exit 1
    else
        if [ "X$WT_VSN" == "X" ]; then
            git clone ${WT_REPO} && \
                (cd $BASEDIR/wiredtiger && git checkout $WT_VSN || exit 1)
        else
            git clone -b ${WT_BRANCH} --single-branch ${WT_REPO} && \
                (cd $BASEDIR/wiredtiger && git checkout -b $WT_BRANCH origin/$WT_BRANCH || exit 1)
        fi
        mv wiredtiger $WT_DIR || exit 1
    fi
    [ -d $BASEDIR/$WT_DIR ] || (echo "Missing WiredTiger source directory" && exit 1)
    (cd $BASEDIR/$WT_DIR && git cherry-pick a3c8c2a13758ae9c44edabcc1a780984a7882904 || exit 1)
    (cd $BASEDIR/$WT_DIR
        [ -e $BASEDIR/wiredtiger-build.patch ] && \
            (patch -p1 --forward < $BASEDIR/wiredtiger-build.patch || exit 1 )
        ./autogen.sh || exit 1
        cd ./build_posix || exit 1
        [ -e Makefile ] && $MAKE distclean
        ../configure --with-pic \
                     --enable-snappy \
                     --enable-bzip2 \
                     --prefix=${BASEDIR}/system || exit 1
    )
}

get_snappy ()
{
    [ -e snappy-$SNAPPY_VSN.tar.gz ] || (echo "Missing Snappy ($SNAPPY_VSN) source package" && exit 1)
    [ -d $BASEDIR/$SNAPPY_DIR ] || tar -xzf snappy-$SNAPPY_VSN.tar.gz
    [ -e $BASEDIR/snappy-build.patch ] && \
        (cd $BASEDIR/$SNAPPY_DIR
        patch -p1 --forward < $BASEDIR/snappy-build.patch || exit 1)
    (cd $BASEDIR/$SNAPPY_DIR
        ./configure --with-pic --prefix=$BASEDIR/system || exit 1)
}

get_bzip2 ()
{
    [ -e bzip2-$BZIP2_VSN.tar.gz  ] || (echo "Missing bzip2 ($BZIP2_VSN) source package" && exit 1)
    [ -d $BASEDIR/$BZIP2_DIR ] || tar -xzf bzip2-$BZIP2_VSN.tar.gz
    [ -e $BASEDIR/bzip2-build.patch ] && \
        (cd $BASEDIR/$BZIP2_DIR
        patch -p1 --forward < $BASEDIR/bzip2-build.patch || exit 1)
}

get_deps ()
{
    get_wt;
    get_snappy;
    get_bzip2;
}

update_deps ()
{
    if [ -d $BASEDIR/$WT_DIR/.git ]; then
        (cd $BASEDIR/$WT_DIR
            if [ "X$WT_VSN" == "X" ]; then
                git pull -u || exit 1
            else
                git checkout $WT_VSN || exit 1
            fi
        )
    fi
}

build_wt ()
{
    (cd $BASEDIR/$WT_DIR/build_posix && \
        $MAKE -j && $MAKE install)
}

build_snappy ()
{
    (cd $BASEDIR/$SNAPPY_DIR && \
        $MAKE -j && \
        $MAKE install
    )
}

build_bzip2 ()
{
    (cd $BASEDIR/$BZIP2_DIR && \
        $MAKE -j -f Makefile-libbz2_so && \
        mkdir -p $BASEDIR/system/lib && \
        cp -f bzlib.h $BASEDIR/system/include && \
        cp -f libbz2.so.1.0.6 $BASEDIR/system/lib && \
        ln -s $BASEDIR/system/lib/libbz2.so.1.0.6 $BASEDIR/system/lib/libbz2.so && \
        ln -s $BASEDIR/system/lib/libbz2.so.1.0.6 $BASEDIR/system/lib/libbz2-1.so && \
        ln -s $BASEDIR/system/lib/libbz2.so.1.0.6 $BASEDIR/system/lib/libbz2-1.0.so
    )
}

case "$1" in
    clean)
        rm -rf system $WT_DIR $SNAPPY_DIR $BZIP2_DIR
        rm -f ${BASEDIR}/../priv/wt
        rm -f ${BASEDIR}/../priv/libwiredtiger-*.so
        rm -f ${BASEDIR}/../priv/libwiredtiger_*.so
        rm -f ${BASEDIR}/../priv/libbz2.so.*
        rm -f ${BASEDIR}/../priv/libsnappy.so.*
        ;;

    test)
        (cd $BASEDIR/$WT_DIR && $MAKE -j test)
        ;;

    update-deps)
        update-deps;
        ;;

    get-deps)
        get_deps;
        ;;

    *)
        [ -d $WT_DIR ] || get_wt;
        [ -d $SNAPPY_DIR ] || get_snappy;
        [ -d $BZIP2_DIR ] || get_bzip2;

        # Build Snappy
        [ -d $BASEDIR/$SNAPPY_DIR ] || (echo "Missing Snappy source directory" && exit 1)
        test -f $BASEDIR/system/lib/libsnappy.so.[0-9].[0-9].[0-9] || build_snappy;

        # Build BZIP2
        [ -d $BASEDIR/$BZIP2_DIR ] || (echo "Missing BZip2 source directory" && exit 1)
        test -f $BASEDIR/system/lib/libbz2.so.[0-9].[0-9].[0-9] || build_bzip2;

        # Build WiredTiger
        [ -d $BASEDIR/$WT_DIR ] || (echo "Missing WiredTiger source directory" && exit 1)
        test -f $BASEDIR/system/lib/libwiredtiger-[0-9].[0-9].[0-9].so \
             -a -f $BASEDIR/system/lib/libwiredtiger_snappy.so \
             -a -f $BASEDIR/system/lib/libwiredtiger_bzip2.so.[0-9].[0-9].[0-9] || build_wt;

        [ -d $BASEDIR/../priv ] || mkdir ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/bin/wt ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libwiredtiger-[0-9].[0-9].[0-9].so ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libwiredtiger_snappy.so ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libwiredtiger_bzip2.so* ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libbz2.so.[0-9].[0-9].[0-9] ${BASEDIR}/../priv
        (cd ${BASEDIR}/../priv
            [ -L libbz2.so ] || ln -s libbz2.so.1.0.6 libbz2.so
            [ -L libbz2.so.1 ] || ln -s libbz2.so.1.0.6 libbz2.so.1
            [ -L libbz2.so.1.0 ] || ln -s libbz2.so.1.0.6 libbz2.so.1.0)
        cp -p -P $BASEDIR/system/lib/libsnappy.so* ${BASEDIR}/../priv
        ;;
esac
