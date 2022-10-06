CHARMC?=../../../../bin/charmc $(OPTS)

OBJS = pagerank.o

all: pagerank

pagerank: $(OBJS)
	$(CHARMC) -language charm++ -o pagerank $(OBJS)

pagerank.decl.h: pagerank.ci
	$(CHARMC)  pagerank.ci

clean:
	rm -f *.decl.h *.def.h conv-host *.o pagerank charmrun

pagerank.o: pagerank.C pagerank.decl.h
	$(CHARMC) -c pagerank.C
