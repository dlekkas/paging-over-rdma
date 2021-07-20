EXTRA_CFLAGS += -D_GNU_SOURCE
obj-m := stackbd.o

all:
	make CC=gcc-4.8 -C /usr/src/linux-headers-$(shell uname -r) M=$(shell pwd) modules

clean:
	make CC=gcc-4.8 -C /usr/src/linux-headers-$(shell uname -r) M=$(shell pwd) clean
