CC=gcc
CFLAGS=-I. -g
LIBS=-llustreapi -lpthread
ODIR=./build

%.o: %.c $(DEPS)
	$(CC) -c -o $(ODIR)/$@ $< $(CFLAGS)

OBJ = $(pathsubst %, $(ODIR)/%)

lhsm_import: lhsm_import.o
	$(CC) -o $(ODIR)/lhsm_import $(ODIR)/lhsm_import.o $(LIBS)

clean:
	rm -fr build/*

