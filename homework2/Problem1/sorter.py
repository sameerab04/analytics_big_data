#!/usr/bin/env python3
  
# import sys because we need to read and write data to STDIN and STDOUT
import sys

all_lines = []

# reading entire line from STDIN (standard input)
for line in sys.stdin:
    # to remove leading and trailing whitespace
    line = line.strip()
    # append this line to our list of lines
    all_lines.append(line)
    
    # we are looping over the lines array and printing the line
    # in the new order to the STDOUT
all_lines.sort()
for line in all_lines:
    # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
    print(line)
