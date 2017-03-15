PROG=xml_formatter.so
CC=cc

%.so : CFLAGS=-Wall -shared

LD = -L$(shell pg_config --libdir) -L$(shell pg_config --pkglibdir) -lpthread -lmxml
PGINC = $(shell pg_config --includedir)
INCLUDEDIRS = -I$(PGINC) -I$(PGINC)/postgresql/internal -I$(PGINC)/postgresql/server

lib/%.o : CFLAGS=-fpic -Wall $(INCLUDEDIRS) 

all: lib/$(PROG)

lib/$(PROG): lib/$(PROG:%.so=%.o)
	$(CC) $(LD) $(CFLAGS) -o $@ $<

lib/$(PROG:%.so=%.o): src/$(PROG:%.so=%.c)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf lib/*.so
	rm -rf lib/*.o

install:
	cp lib/$(PROG) $(GPHOME)/lib/postgresql
	psql -f sql/install.sql
