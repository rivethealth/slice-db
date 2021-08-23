###
# Config
###

JOBS ?= $(shell nproc)
MAKEFLAGS += -j $(JOBS) -r

PATH := $(abspath node_modules)/.bin:$(PATH)

.DELETE_ON_ERROR:
.SECONDARY:
.SUFFIXES:

LPAREN := (
RPAREN := )

###
# Clean
###

TARGET := mllp_http.egg-info build target

.PHONY: clean
clean:
	rm -fr $(TARGET)

###
# Schema
###

SCHEMA_SRC := $(shell find schema -name '*.yml')
SCHEMA_TGT := $(SCHEMA_SRC:schema/%.yml=slice_db/formats/%.json)

slice_db/formats/%.json: schema/%.yml
	< $< yq > $@

.PHONY: schema
schema: $(SCHEMA_TGT)

###
# Format
###
FORMAT_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')
PRETTIER_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.md')

.PHONY: format
format: target/format.target

.PHONY: test-format
test-format: target/format-test.target

target/format.target: $(FORMAT_SRC) $(PRETTIER_SRC) target/node_modules.target
	mkdir -p $(@D)
	isort --profile black $(FORMAT_SRC)
	black -t py37 $(FORMAT_SRC)
	node_modules/.bin/prettier --write .
	touch $@ target/format-test.target

target/format-test.target: $(FORMAT_SRC)
	mkdir -p $(@D)
	black -t py37 --check $(FORMAT_SRC)
	touch $@ target/format.target

###
# Npm
###
target/node_modules.target:
	mkdir -p $(@D)
	yarn install
	> $@

###
# Pip
###
PY_SRC := $(shell find . $(TARGET:%=-not \$(LPAREN) -name % -prune \$(RPAREN)) -name '*.py')

.PHONY: install
install:
	pip3 install -e .[dev]

.PHONY: package
package: target/package.target

upload: target/package-test.target
	python3 -m twine upload target/package/*

target/package.target: setup.py README.md $(PY_SRC)
	rm -fr $(@:.target=)
	mkdir -p $(@:.target=)
	./$< bdist_wheel -d $(@:.target=) sdist -d $(@:.target=)
	> $@

target/package-test.target: target/package.target
	mkdir -p $(@D)
	python3 -m twine check target/package/*
	mkdir -p $(@D)
	> $@

###
# Docker
###

.PHONY: docker
docker:
	docker build -t rivethealth/slice-db .

###
# Test
###

.PHONY: test
test: $(SCHEMA_TGT)
	pytest
