APPS = client daemon

FLAGS =  -std=c++11 -O2 -Wall
LDFLAGS =   -libverbs -lrdmacm 


all: ${APPS}

daemon: daemon.cpp
	g++ daemon.cpp -o daemon $(INCLUDE) $(FLAGS)  $(LDFLAGS)

client: client.cpp swap.hpp
	g++ client.cpp -o client $(INCLUDE) $(FLAGS)  $(LDFLAGS)

clean:
	$(foreach fname,${APPS}, rm -f ${fname})

.DELETE_ON_ERROR:
.PHONY: all clean