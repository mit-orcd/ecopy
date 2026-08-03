CC       ?= gcc
CPPFLAGS ?=
CFLAGS   ?= -O3 -Wall -Wextra -pthread
LDFLAGS  ?= -pthread
DEPFLAGS ?= -MMD -MP
PKG_CONFIG ?= pkg-config

TARGET = ecopy
JEMALLOC_TARGET = ecopy-jemalloc
ARCH := $(shell uname -m)

BLAKE3_OBJS = \
	third_party/blake3/blake3.o \
	third_party/blake3/blake3_dispatch.o \
	third_party/blake3/blake3_portable.o \
	third_party/blake3/blake3_wrapper.o

ifneq ($(filter x86_64 amd64 i386 i686,$(ARCH)),)
BLAKE3_OBJS += \
	third_party/blake3/blake3_sse2.o \
	third_party/blake3/blake3_sse41.o \
	third_party/blake3/blake3_avx2.o \
	third_party/blake3/blake3_avx512.o
else
BLAKE3_CPPFLAGS = -DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 \
	-DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512 -DBLAKE3_USE_NEON=0
# Upstream defines get_cpu_features() unconditionally but only calls it from the
# x86 dispatch paths, so it is dead code on every other arch.
third_party/blake3/blake3_dispatch.o: override CFLAGS += -Wno-unused-function
endif

OBJS = \
	main.o \
	traversal.o \
	workers.o \
	stats.o \
	progress.o \
	telemetry.o \
	fs_util.o \
	hardlinks.o \
	copy_policy.o \
	verify.o \
	$(BLAKE3_OBJS) \
	suggestion.o \
	protocol.o \
	ssh_transport.o \
	server.o

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

.PHONY: all clean test protocol_test telemetry_test blake3_bench

all: $(TARGET) $(OPTIONAL_TARGETS)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(DEPFLAGS) -c -o $@ $<

third_party/blake3/blake3.o third_party/blake3/blake3_dispatch.o \
third_party/blake3/blake3_portable.o third_party/blake3/blake3_wrapper.o: \
CPPFLAGS += $(BLAKE3_CPPFLAGS)

third_party/blake3/blake3_sse2.o: override CFLAGS += -msse2
third_party/blake3/blake3_sse41.o: override CFLAGS += -mssse3 -msse4.1
third_party/blake3/blake3_avx2.o: override CFLAGS += -mavx2
third_party/blake3/blake3_avx512.o: override CFLAGS += -mavx512f -mavx512vl

ifeq ($(HAVE_JEMALLOC),yes)
$(JEMALLOC_TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(JEMALLOC_CFLAGS) -o $@ $(OBJS) $(LDFLAGS) $(JEMALLOC_LIBS)
else
$(JEMALLOC_TARGET):
	@echo "jemalloc headers/libs were not found; $@ was not built"
	@false
endif

protocol_test: tests/protocol_test.c protocol.o $(BLAKE3_OBJS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ tests/protocol_test.c protocol.o $(BLAKE3_OBJS) $(LDFLAGS)

telemetry_test: tests/telemetry_test.c telemetry.o stats.o $(BLAKE3_OBJS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ tests/telemetry_test.c telemetry.o stats.o $(BLAKE3_OBJS) $(LDFLAGS)

blake3_bench: tests/blake3_bench.c $(BLAKE3_OBJS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ tests/blake3_bench.c $(BLAKE3_OBJS) $(LDFLAGS)

clean:
	rm -f *.o *.d third_party/blake3/*.o third_party/blake3/*.d $(TARGET) $(JEMALLOC_TARGET) direct_copy protocol_test telemetry_test blake3_bench
	rm -rf *.dSYM   # debug bundles, emitted when linking with -g on macOS

test: $(TARGET) protocol_test telemetry_test
	@set -e; \
	echo "==> protocol_test"; \
	./protocol_test; \
	echo "==> telemetry_test"; \
	./telemetry_test; \
	for t in tests/*.sh; do \
		echo "==> $$t"; \
		bash "$$t"; \
	done

-include $(OBJS:.o=.d)
