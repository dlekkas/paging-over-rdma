TARGET = example

SRCS = $(shell find . -not -path '*/\.*' -type f -name *.cc)
OBJS = $(SRCS:.cc=.o)

CXXFLAGS = -O1 -Wall -std=c++17

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) -o $@ $(OBJS)

run: all
	@./$(TARGET)

.PHONY: clean

clean:
	rm -f $(OBJS) $(TARGET)
