TARGET=		wterl

REBAR=		./rebar
#REBAR=		/usr/bin/env rebar
ERL=		/usr/bin/env erl
DIALYZER=	/usr/bin/env dialyzer


.PHONY: plt analyze all deps compile get-deps clean

all: compile

deps: get-deps

get-deps:
	@$(REBAR) get-deps

update-deps:
	@$(REBAR) update-deps

c_src/wterl.o:
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
	@$(ERL) -pa .eunit deps/*/ebin

plt: compile
	@$(DIALYZER) --build_plt --output_plt .$(TARGET).plt -pa deps/*/ebin --apps kernel stdlib

analyze: compile
	$(DIALYZER) --plt .$(TARGET).plt -pa deps/*/ebin ebin

repl:
	$(ERL) -pz deps/*/ebin -pa ebin

gdb-repl:
	USE_GDB=1 $(ERL) -pz deps/*/ebin -pa ebin

eunit-repl:
	$(ERL) -pa .eunit -pz deps/*/ebin -pz ebin -exec 'cd(".eunit").'

gdb-eunit-repl:
	USE_GDB=1 $(ERL) -pa .eunit -pz deps/*/ebin -pz ebin -exec 'cd(".eunit").'
