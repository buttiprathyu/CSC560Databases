# Makefile for the Minibase project.  Needs GNU make.
#
# Warning: make depend overwrites this file.

.PHONY: depend clean backup setup

MAIN=hfpage

MINIBASE=..

CC=g++

CFLAGS= -DUNIX -Wall -g 

INCLUDES = -I${MINIBASE}/include -I.

#LFLAGS= -L${MINIBASE}/lib -liberty -lm
#LFLAGS= -L${MINIBASE}/lib32 -lbm -ldb -lm
LFLAGS= -L${MINIBASE}/lib32 -L${MINIBASE}/lib -lbm -ldb -lm -no-pie

SRCS = main.C hfpage.C hfp_driver.C test_driver.C \
		new_error.C page.C system_defs.C

OBJS = $(SRCS:.C=.o)

$(MAIN):  $(OBJS)
	 $(CC) $(CFLAGS) $(INCLUDES) $(OBJS) -o $(MAIN) $(LFLAGS)

.C.o:
	$(CC) $(CFLAGS) $(INCLUDES) $(LFLAGS) -c $<

depend: $(SRCS)
	makedepend $(INCLUDES) $^

clean:
	rm -f *.o *~ $(MAIN) $(MAKECLEANGARBAGE)

backup:
	mkdir bak
	cp Makefile *.[Ch] bak

# Grab the sources for a user who has only the makefile
setup:
	/bin/cp -i $(MINIBASE)/src/*.[Ch] .
	-/bin/cp -i $(MINIBASE)/src/README .

# DO NOT DELETE THIS LINE -- make depend needs it

main.o: /usr/include/stdlib.h ../include/hfp_driver.h
main.o: ../include/test_driver.h ../include/minirel.h ../include/da_types.h
main.o: ../include/new_error.h /usr/include/assert.h /usr/include/features.h
main.o: /usr/include/stdc-predef.h ../include/system_defs.h
hfpage.o: /usr/include/stdlib.h /usr/include/memory.h /usr/include/features.h
hfpage.o: /usr/include/stdc-predef.h /usr/include/string.h
hfpage.o: /usr/include/strings.h ../include/hfpage.h ../include/minirel.h
hfpage.o: ../include/da_types.h ../include/new_error.h /usr/include/assert.h
hfpage.o: ../include/system_defs.h ../include/page.h ../include/buf.h
hfpage.o: ../include/db.h
hfp_driver.o: /usr/include/stdio.h /usr/include/stdlib.h
hfp_driver.o: /usr/include/unistd.h /usr/include/features.h
hfp_driver.o: /usr/include/stdc-predef.h /usr/include/string.h
hfp_driver.o: /usr/include/strings.h /usr/include/assert.h ../include/db.h
hfp_driver.o: ../include/page.h ../include/minirel.h ../include/da_types.h
hfp_driver.o: ../include/new_error.h ../include/system_defs.h
hfp_driver.o: ../include/scan.h ../include/hfp_driver.h
hfp_driver.o: ../include/test_driver.h ../include/buf.h ../include/hfpage.h
test_driver.o: /usr/include/stdlib.h /usr/include/stdio.h
test_driver.o: /usr/include/string.h /usr/include/strings.h
test_driver.o: /usr/include/features.h /usr/include/stdc-predef.h
test_driver.o: /usr/include/errno.h /usr/include/assert.h
test_driver.o: ../include/new_error.h ../include/test_driver.h
test_driver.o: ../include/minirel.h ../include/da_types.h
test_driver.o: ../include/system_defs.h /usr/include/pwd.h
test_driver.o: /usr/include/unistd.h
new_error.o: ../include/new_error.h /usr/include/assert.h
new_error.o: /usr/include/features.h /usr/include/stdc-predef.h
new_error.o: /usr/include/string.h /usr/include/strings.h
new_error.o: /usr/include/stdio.h /usr/include/stdlib.h
page.o: ../include/page.h ../include/minirel.h ../include/da_types.h
page.o: ../include/new_error.h /usr/include/assert.h /usr/include/features.h
page.o: /usr/include/stdc-predef.h ../include/system_defs.h
system_defs.o: /usr/include/stdio.h ../include/minirel.h
system_defs.o: ../include/da_types.h ../include/new_error.h
system_defs.o: /usr/include/assert.h /usr/include/features.h
system_defs.o: /usr/include/stdc-predef.h ../include/system_defs.h
system_defs.o: ../include/db.h /usr/include/string.h /usr/include/strings.h
system_defs.o: /usr/include/stdlib.h ../include/page.h ../include/buf.h
