.PHONY: clean

LDLIBS := ${LDLIBS} -lrdmacm -libverbs -lpthread
CC := g++

SERVER := server

all: ${SERVER}

clean:
	rm -f ${SERVER}
