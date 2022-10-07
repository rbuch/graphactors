CHARMC?=../../../../bin/charmc $(OPTS)

all: pagerank labelprop

pagerank: pagerank.o
	$(CHARMC) -language charm++ -o $@ $^

labelprop: labelprop.o
	$(CHARMC) -language charm++ -o $@ $^

pagerank.decl.h: pagerank.ci
	$(CHARMC)  $^

labelprop.decl.h: labelprop.ci
	$(CHARMC)  $^

clean:
	rm -f *.decl.h *.def.h conv-host *.o labelprop pagerank charmrun

pagerank.o: pagerank.C pagerank.decl.h
	$(CHARMC) -c $<

labelprop.o: labelprop.C labelprop.decl.h
	$(CHARMC) -c $<
