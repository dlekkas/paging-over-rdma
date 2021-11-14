TARGET = example example_strided

SRCS = $(shell find . -not -path '*/\.*' -type f -name *.cc)
OBJS = $(SRCS:.cc=.o)

CXXFLAGS = -O1 -Wall -std=c++17

all: $(TARGET)

example: example.o
	$(CXX) -o $@ example.o

example_strided: example_strided.o
	$(CXX) -o $@ example_strided.o

run: all
	@./$(TARGET)

.PHONY: clean

clean:
	rm -f $(OBJS) $(TARGET)
