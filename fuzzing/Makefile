.PHONY: all clean

all:
	./generate_packet_corpora.py
	$(MAKE) -C broker $@

clean:
	-rm -rf corpora/broker corpora/client corpora/broker_packet_seed_corpus.zip corpora/client_packet_seed_corpus.zip
	$(MAKE) -C broker $@
