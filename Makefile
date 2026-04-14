CC      = gcc
CFLAGS  = -O3 -Wall -Wextra -pthread
LDFLAGS = -pthread

OBJS = \
	main.o \
	traversal.o \
	workers.o \
	stats.o \
	progress.o \
	fs_util.o \
	suggestion.o

all: direct_copy

direct_copy: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

clean:
	rm -f *.o direct_copy
