TARGET = server

# ignore hidden files/directories (messes up with .ccls_cache)
SRCS = $(shell find ./src -not -path '*/\.*' -type f -name *.cc)
OBJS = $(SRCS:.cc=.o)

CXXFLAGS = -O3 -Wall -std=c++17
LDFLAGS = -lrdmacm -libverbs -lpthread

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) -o $@ $(OBJS) ${LDFLAGS}

run: all
	@./$(TARGET)


.PHONY: clean

clean:
	rm -f $(OBJS) $(TARGET)
