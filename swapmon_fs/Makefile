EXTRA_CFLAGS += -D_GNU_SOURCE
KDIR ?= /lib/modules/$(shell uname -r)/build

obj-m := mcswap.o

all:
	$(info EXTRA_CFLAGS = $(EXTRA_CFLAGS))
	make -C $(KDIR) M=$(shell pwd) modules

clean:
	make -C $(KDIR) M=$(shell pwd) clean
