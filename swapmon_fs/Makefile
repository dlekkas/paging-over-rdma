EXTRA_CFLAGS += -D_GNU_SOURCE -DBENCH -DASYNC
KDIR ?= /lib/modules/$(shell uname -r)/build

obj-m := mcswap.o

all:
	make -C $(KDIR) M=$(shell pwd) modules

clean:
	make -C $(KDIR) M=$(shell pwd) clean
