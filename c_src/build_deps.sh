#!/bin/bash

#t=__.$$
#trap 'rm -f $t; exit 0' 0 1 2 3 13 15

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

set -e

# WiredTiger
WT_REPO=http://github.com/wiredtiger/wiredtiger.git
#WT_BRANCH=develop
#WT_DIR=wiredtiger-`basename $WT_BRANCH`
WT_REF="tags/1.6.3"
WT_DIR=wiredtiger-`basename $WT_REF`

# Google's Snappy Compression
SNAPPY_VSN="1.0.4"
SNAPPY_DIR=snappy-$SNAPPY_VSN

# User-space Read-Copy-Update (RCU)
URCU_REPO=git://git.lttng.org/userspace-rcu.git
URCU_REF="tags/v0.7.7"
URCU_DIR=urcu-`basename $URCU_REF`

[ `basename $PWD` != "c_src" ] && cd c_src

export BASEDIR="$PWD"

which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$BASEDIR/system/lib:$LD_LIBRARY_PATH"

get_urcu ()
{
    if [ -d $BASEDIR/$URCU_DIR/.git ]; then
        (cd $BASEDIR/$URCU_DIR && git pull -u) || exit 1
    else
        if [ "X$URCU_REF" != "X" ]; then
            git clone ${URCU_REPO} ${URCU_DIR} && \
                (cd $BASEDIR/$URCU_DIR && git checkout refs/$URCU_REF || exit 1)
        else
            git clone ${URCU_REPO} ${URCU_DIR} && \
                (cd $BASEDIR/$URCU_DIR && git checkout -b $URCU_BRANCH origin/$URCU_BRANCH || exit 1)
        fi
    fi
    [ -d $BASEDIR/$URCU_DIR ] || (echo "Missing WiredTiger source directory" && exit 1)
    (cd $BASEDIR/$URCU_DIR
        [ -e $BASEDIR/urcu-build.patch ] && \
            (patch -p1 --forward < $BASEDIR/urcu-build.patch || exit 1 )
        autoreconf -fis || exit 1
        urcu_configure;
    )
}

urcu_configure ()
{
    (cd $BASEDIR/$URCU_DIR
	CFLAGS+="-m64 -Os -g -march=native -mtune=native" \
            ./configure --disable-shared --prefix=${BASEDIR}/system || exit 1)
}

get_wt ()
{
    if [ -d $BASEDIR/$WT_DIR/.git ]; then
        (cd $BASEDIR/$WT_DIR && git pull -u) || exit 1
    else
        if [ "X$WT_REF" != "X" ]; then
            git clone ${WT_REPO} ${WT_DIR} && \
                (cd $BASEDIR/$WT_DIR && git checkout refs/$WT_REF || exit 1)
        else
            git clone ${WT_REPO} ${WT_DIR} && \
                (cd $BASEDIR/$WT_DIR && git checkout -b $WT_BRANCH origin/$WT_BRANCH || exit 1)
        fi
    fi
    [ -d $BASEDIR/$WT_DIR ] || (echo "Missing WiredTiger source directory" && exit 1)
    (cd $BASEDIR/$WT_DIR
        [ -e $BASEDIR/wiredtiger-build.patch ] && \
            (patch -p1 --forward < $BASEDIR/wiredtiger-build.patch || exit 1 )
        ./autogen.sh || exit 1
        [ -e $BASEDIR/$WT_DIR/build_posix/Makefile ] && \
            (cd $BASEDIR/$WT_DIR/build_posix && $MAKE distclean)
        wt_configure;
    )
}

wt_configure ()
{
    (cd $BASEDIR/$WT_DIR/build_posix
        CFLAGS+=-g ../configure --with-pic \
                     --enable-snappy \
                     --disable-python --disable-java \
                     --prefix=${BASEDIR}/system || exit 1)
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

get_deps ()
{
    get_wt;
    get_snappy;
    get_urcu;
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

    if [ -d $BASEDIR/$URCU_DIR/.git ]; then
        (cd $BASEDIR/$URCU_DIR
            if [ "X$URCU_VSN" == "X" ]; then
                git pull -u || exit 1
            else
                git checkout $URCU_VSN || exit 1
            fi
        )
    fi
}

build_urcu ()
{
    urcu_configure;
    (cd $BASEDIR/$URCU_DIR && $MAKE -j && $MAKE install)
}

build_wt ()
{
    wt_configure;
    (cd $BASEDIR/$WT_DIR/build_posix && $MAKE -j && $MAKE install)
}

build_snappy ()
{
    (cd $BASEDIR/$SNAPPY_DIR && \
        $MAKE -j && \
        $MAKE install
    )
}

case "$1" in
    clean)
        #[ -e $BASEDIR/$WT_DIR/build_posix/Makefile ] && (cd $BASEDIR/$WT_DIR/build_posix && $MAKE clean)
        [ -e $BASEDIR/$URCU_DIR/Makefile ] && (cd $BASEDIR/$URCU_DIR && $MAKE clean)
        rm -rf system $SNAPPY_DIR
        rm -f ${BASEDIR}/../priv/wt
        rm -f ${BASEDIR}/../priv/libwiredtiger-*.so
        rm -f ${BASEDIR}/../priv/libwiredtiger_*.so
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
        [ -d $URCU_DIR ] || get_urcu;
        [ -d $WT_DIR ] || get_wt;
        [ -d $SNAPPY_DIR ] || get_snappy;

        # Build URCU
        [ -d $BASEDIR/$URCU_DIR ] || (echo "Missing URCU source directory" && exit 1)
        test -f $BASEDIR/system/lib/liburcu-*.a || build_urcu;

        # Build Snappy
        [ -d $BASEDIR/$SNAPPY_DIR ] || (echo "Missing Snappy source directory" && exit 1)
        test -f $BASEDIR/system/lib/libsnappy.so.[0-9].[0-9].[0-9] || build_snappy;

        # Build WiredTiger
        [ -d $BASEDIR/$WT_DIR ] || (echo "Missing WiredTiger source directory" && exit 1)
        test -f $BASEDIR/system/lib/libwiredtiger-[0-9].[0-9].[0-9].so \
             -a -f $BASEDIR/system/lib/libwiredtiger_snappy.so || build_wt;

        [ -d $BASEDIR/../priv ] || mkdir ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/bin/wt ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libwiredtiger-[0-9].[0-9].[0-9].so ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libwiredtiger_snappy.so* ${BASEDIR}/../priv
        cp -p -P $BASEDIR/system/lib/libsnappy.so* ${BASEDIR}/../priv
        ;;
esac

exit 0
