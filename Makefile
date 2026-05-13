CC       ?= gcc
CPPFLAGS ?=
CFLAGS   ?= -O3 -Wall -Wextra -pthread
LDFLAGS  ?= -pthread
PKG_CONFIG ?= pkg-config

TARGET = ecopy
JEMALLOC_TARGET = ecopy-jemalloc

OBJS = \
	main.o \
	traversal.o \
	workers.o \
	stats.o \
	progress.o \
	fs_util.o \
	suggestion.o

ifneq ($(MAKECMDGOALS),clean)
JEMALLOC_PKGCONFIG := $(shell command -v $(PKG_CONFIG) >/dev/null 2>&1 && $(PKG_CONFIG) --exists jemalloc && echo yes)
ifeq ($(JEMALLOC_PKGCONFIG),yes)
HAVE_JEMALLOC := yes
JEMALLOC_CFLAGS := $(shell $(PKG_CONFIG) --cflags jemalloc)
JEMALLOC_LIBS := $(shell $(PKG_CONFIG) --libs jemalloc)
else
JEMALLOC_CHECK := $(shell tmp=$$(mktemp "$${TMPDIR:-/tmp}/ecopy-jemalloc-check.XXXXXX" 2>/dev/null) || exit 0; printf '\043include <jemalloc/jemalloc.h>\nint main\050void\051 { return 0; }\n' | $(CC) $(CPPFLAGS) $(CFLAGS) -x c - -o "$$tmp" -ljemalloc >/dev/null 2>&1; status=$$?; rm -f "$$tmp"; [ $$status -eq 0 ] && echo yes)
ifeq ($(JEMALLOC_CHECK),yes)
HAVE_JEMALLOC := yes
JEMALLOC_CFLAGS :=
JEMALLOC_LIBS := -ljemalloc
endif
endif
endif

OPTIONAL_TARGETS =
ifeq ($(HAVE_JEMALLOC),yes)
OPTIONAL_TARGETS += $(JEMALLOC_TARGET)
endif

.PHONY: all clean test

all: $(TARGET) $(OPTIONAL_TARGETS)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<

ifeq ($(HAVE_JEMALLOC),yes)
$(JEMALLOC_TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) -o $@ $(OBJS) $(LDFLAGS) $(JEMALLOC_LIBS)
else
$(JEMALLOC_TARGET):
	@echo "jemalloc headers/libs were not found; $@ was not built"
	@false
endif

clean:
	rm -f *.o $(TARGET) $(JEMALLOC_TARGET) direct_copy

test: $(TARGET)
	@set -e; \
	for t in tests/*.sh; do \
		echo "==> $$t"; \
		bash "$$t"; \
	done
