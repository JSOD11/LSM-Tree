CC=gcc
CFLAGS=-I/opt/homebrew/opt/gsl/include -g
LDFLAGS=-L/opt/homebrew/opt/gsl/lib -lgsl -lgslcblas

all: generator 

generator: generator.c
	$(CC) $(CFLAGS) $< -o $@ $(LDFLAGS)
	chmod +x $@

clean:
	rm -f generator
