CHARMC?=../../../../bin/charmc $(OPTS)

all: pagerank labelprop labelprop_chunk

pagerank: pagerank.o
	$(CHARMC) -language charm++ -o $@ $^

labelprop: labelprop.o
	$(CHARMC) -language charm++ -o $@ $^

labelprop_chunk: labelprop_chunk.o
	$(CHARMC) -language charm++ -o $@ $^

pagerank.decl.h: pagerank.ci
	$(CHARMC)  $^

labelprop.decl.h: labelprop.ci
	$(CHARMC)  $^

labelprop_chunk.decl.h: labelprop_chunk.ci
	$(CHARMC)  $^

clean:
	rm -f *.decl.h *.def.h conv-host *.o labelprop labelprop_chunk pagerank charmrun

pagerank.o: pagerank.C pagerank.decl.h
	$(CHARMC) -c $<

labelprop.o: labelprop.C labelprop.decl.h
	$(CHARMC) -c $<

labelprop_chunk.o: labelprop_chunk.C labelprop_chunk.decl.h
	$(CHARMC) -c $<
