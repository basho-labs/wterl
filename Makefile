TARGET=		wterl

REBAR=		./rebar
#REBAR=		/usr/bin/env rebar
ERL=		/usr/bin/env erl
ERLEXEC=	${ERL_ROOTDIR}/lib/erlang/erts-5.9.1/bin/erlexec
DIALYZER=	/usr/bin/env dialyzer
ARCHIVETAG?=	$(shell git describe --always --long --tags)
ARCHIVE?=	$(shell basename $(CURDIR))-$(ARCHIVETAG)
WT_ARCHIVETAG?=	$(shell cd c_src/wiredtiger-basho; git describe --always --long --tags)

.PHONY: plt analyze all deps compile get-deps clean

all: compile

archive:
	@rm -f $(ARCHIVE).tar.gz
	git archive --format=tar --prefix=$(ARCHIVE)/ $(ARCHIVETAG) | gzip >$(ARCHIVE).tar.gz

deps: get-deps

get-deps:
	c_src/build_deps.sh get-deps
	@$(REBAR) get-deps

update-deps:
	c_src/build_deps.sh update-deps
	@$(REBAR) update-deps

c_src/wterl.o: c_src/async_nif.h
	touch c_src/wterl.c

ebin/app_helper.beam:
	@echo You need to:
	@echo cp ../riak/deps/riak_core/ebin/app_helper.beam ebin
	@/bin/false

compile: c_src/wterl.o ebin/app_helper.beam
	@$(REBAR) compile

clean:
	@rm -f $(ARCHIVE).tar.gz
	@$(REBAR) clean

xref:
	@$(REBAR) xref skip_deps=true

test: eunit

eunit: compile-for-eunit
	@$(REBAR) eunit skip_deps=true

eqc: compile-for-eqc
	@$(REBAR) eqc skip_deps=true

proper: compile-for-proper
	@echo "rebar does not implement a 'proper' command" && false

triq: compile-for-triq
	@$(REBAR) triq skip_deps=true

compile-for-eunit:
	@$(REBAR) compile eunit compile_only=true

compile-for-eqc:
	@$(REBAR) -D QC -D QC_EQC compile eqc compile_only=true

compile-for-eqcmini:
	@$(REBAR) -D QC -D QC_EQCMINI compile eqc compile_only=true

compile-for-proper:
	@$(REBAR) -D QC -D QC_PROPER compile eqc compile_only=true

compile-for-triq:
	@$(REBAR) -D QC -D QC_TRIQ compile triq compile_only=true

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa deps/lager/ebin --apps kernel stdlib

analyze: compile
	@$(DIALYZER) --plt .$(TARGET).plt -pa deps/lager/ebin ebin

repl:
	@$(ERL) -pa ebin -pz deps/lager/ebin

eunit-repl:
	@$(ERL) erl -pa .eunit -pz deps/lager/ebin

ERL_TOP=		/home/gburd/repos/otp_R15B01
CERL=			${ERL_TOP}/bin/cerl
VALGRIND_MISC_FLAGS=	"--verbose --leak-check=full --show-reachable=yes --trace-children=yes --track-origins=yes --suppressions=${ERL_TOP}/erts/emulator/valgrind/suppress.standard --show-possibly-lost=no --malloc-fill=AB --free-fill=CD"

helgrind:
	valgrind --verbose --tool=helgrind \
	            --leak-check=full
	            --show-reachable=yes \
	            --trace-children=yes \
	            --track-origins=yes \
	            --suppressions=${ERL_TOP}/erts/emulator/valgrind/suppress.standard \
	            --show-possibly-lost=no \
	            --malloc-fill=AB \
	            --free-fill=CD ${ERLEXEC} -pz deps/lager/ebin -pa ebin -pa .eunit

valgrind:
	${CERL} -valgrind ${VALGRIND_FLAGS} --log-file=${ROOTDIR}/valgrind_log-beam.smp.%p -- -pz deps/lager/ebin -pa ebin -pa .eunit -exec 'eunit:test(wterl).'
