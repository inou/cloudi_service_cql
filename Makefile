all: deps compile

deps:
	@ rebar get-deps

compile:
	@ rebar compile

clean:
	@ rebar clean

console: compile
	@ erl -pa ebin deps/*/ebin test

test: compile
	@ rebar skip_deps=true ct
