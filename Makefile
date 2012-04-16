JS_TESTER = ./node_modules/vows/bin/vows
PEG_COMPILER = ./node_modules/pegjs/bin/pegjs

.PHONY: test

%.js: %.peg Makefile
	$(PEG_COMPILER) < $< > $@

all: \
	lib/cube/event-expression.js \
	lib/cube/metric-expression.js

test: all
	@$(JS_TESTER)
