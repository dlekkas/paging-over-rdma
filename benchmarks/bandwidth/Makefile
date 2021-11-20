TARGET = page_strided

SRCS = $(shell find . -not -path '*/\.*' -type f -name *.cc)
OBJS = $(SRCS:.cc=.o)

CXXFLAGS = -O1 -Wall -std=c++17

all: $(TARGET)

page_strided: page_strided.o
	$(CXX) -o $@ page_strided.o

run: all
	@./$(TARGET)

.PHONY: clean

clean:
	rm -f $(OBJS) $(TARGET)
