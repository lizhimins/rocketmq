# Makefile for fast-commitlog user-space library and demo
#
# Build:
#   make
#
# Install:
#   make install PREFIX=/usr/local
#
# Clean:
#   make clean

CC      ?= gcc
CFLAGS  ?= -Wall -Wextra -O2 -g
PREFIX  ?= /usr/local

INCLUDE_DIR := include
SRC_DIR     := userspace
BUILD_DIR   := build

LIB_SRC  := $(SRC_DIR)/fast_commitlog.c
DEMO_SRC := $(SRC_DIR)/demo_fast_commitlog.c

LIB_OBJ  := $(BUILD_DIR)/fast_commitlog.o
DEMO_OBJ := $(BUILD_DIR)/demo_fast_commitlog.o

LIB_OUT  := $(BUILD_DIR)/libfastcommitlog.a
DEMO_OUT := $(BUILD_DIR)/demo_fast_commitlog

.PHONY: all clean install

all: $(DEMO_OUT)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(LIB_OBJ): $(LIB_SRC) $(INCLUDE_DIR)/fast_commitlog.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(INCLUDE_DIR) -c $< -o $@

$(DEMO_OBJ): $(DEMO_SRC) $(INCLUDE_DIR)/fast_commitlog.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(INCLUDE_DIR) -c $< -o $@

$(LIB_OUT): $(LIB_OBJ)
	ar rcs $@ $^

$(DEMO_OUT): $(DEMO_OBJ) $(LIB_OUT)
	$(CC) $(CFLAGS) $< -L$(BUILD_DIR) -lfastcommitlog -o $@

install: $(LIB_OUT) $(INCLUDE_DIR)/fast_commitlog.h
	install -d $(PREFIX)/lib
	install -d $(PREFIX)/include
	install -m 644 $(LIB_OUT) $(PREFIX)/lib/
	install -m 644 $(INCLUDE_DIR)/fast_commitlog.h $(PREFIX)/include/
	install -m 755 $(DEMO_OUT) $(PREFIX)/bin/

clean:
	rm -rf $(BUILD_DIR)
