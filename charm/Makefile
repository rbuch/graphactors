CHARMC?=../../../../bin/charmc

OPTS?=-O3 -march=native

all: pagerank pagerank_chunk labelprop labelprop_chunk

pagerank: pagerank.o
	$(CHARMC) $(OPTS) -language charm++ -o $@ $^ -module CommonLBs

pagerank_chunk: pagerank_chunk.o
	$(CHARMC) $(OPTS) -language charm++ -o $@ $^ -module CommonLBs

labelprop: labelprop.o
	$(CHARMC) $(OPTS) -language charm++ -o $@ $^ -module CommonLBs

labelprop_chunk: labelprop_chunk.o
	$(CHARMC) $(OPTS) -language charm++ -o $@ $^ -module CommonLBs

pagerank.decl.h: pagerank.ci
	$(CHARMC) $(OPTS)  $^

pagerank_chunk.decl.h: pagerank_chunk.ci
	$(CHARMC) $(OPTS)  $^

labelprop.decl.h: labelprop.ci
	$(CHARMC) $(OPTS)  $^

labelprop_chunk.decl.h: labelprop_chunk.ci
	$(CHARMC) $(OPTS)  $^

clean:
	rm -f *.decl.h *.def.h conv-host *.o labelprop labelprop_chunk pagerank pagerank_chunk charmrun

pagerank.o: pagerank.C pagerank.decl.h
	$(CHARMC) $(OPTS) -c $<

pagerank_chunk.o: pagerank_chunk.C pagerank_chunk.decl.h
	$(CHARMC) $(OPTS) -c $<

labelprop.o: labelprop.C labelprop.decl.h
	$(CHARMC) $(OPTS) -c $<

labelprop_chunk.o: labelprop_chunk.C labelprop_chunk.decl.h
	$(CHARMC) $(OPTS) -c $<
