EXTRA_CFLAGS += -D_GNU_SOURCE -DBENCH
KDIR ?= /lib/modules/$(shell uname -r)/build

obj-m := swapmon.o

all:
	make -C $(KDIR) M=$(shell pwd) modules

clean:
	make -C $(KDIR) M=$(shell pwd) clean
