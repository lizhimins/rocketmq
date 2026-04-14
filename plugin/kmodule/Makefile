# Makefile for ext4_fast_alloc kernel module

KVER := $(shell uname -r)
KDIR := /usr/src/kernels/$(KVER)

obj-m := ext4_fast_alloc.o

# Enable debug prints
ccflags-y := -DDEBUG -g

all:
	$(MAKE) -C $(KDIR) M=$(shell pwd) modules

clean:
	$(MAKE) -C $(KDIR) M=$(shell pwd) clean

# Sign the module with kernel's signing key (optional, suppresses dmesg warning)
sign: all
	$(KDIR)/scripts/sign-file sha256 \
		$(KDIR)/certs/signing_key.pem \
		$(KDIR)/certs/signing_key.x509 \
		ext4_fast_alloc.ko

install: all
	install -m 644 ext4_fast_alloc.ko /lib/modules/$(KVER)/extra/
	depmod -a

load:
	sudo insmod ext4_fast_alloc.ko

unload:
	sudo rmmod ext4_fast_alloc

reload: unload load

status:
	lsmod | grep ext4_fast_alloc
	dmesg | grep ext4_fast_alloc | tail -20
