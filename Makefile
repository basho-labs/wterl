TARGET=		wterl

REBAR=		./rebar
#REBAR=		/usr/bin/env rebar
ERL=		/usr/bin/env erl
DIALYZER=	/usr/bin/env dialyzer


.PHONY: plt analyze all deps compile get-deps clean

all: compile

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
	@$(REBAR) clean

test: eunit

eunit: compile
	@$(REBAR) eunit skip_deps=true

eunit_console:
	@$(ERL) -pa .eunit deps/lager/ebin

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa deps/lager/ebin --apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .$(TARGET).plt -pa deps/lager/ebin ebin

repl:
	$(ERL) -pz deps/lager/ebin -pa ebin

eunit-repl:
	$(ERL) -pz deps/lager/ebin -pa ebin -pa .eunit
