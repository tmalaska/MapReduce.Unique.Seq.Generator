Map Reduce Sequence Generator
-----------------------------

This is a single map reduce that will assign a unique sequence number to every row in the source file.  The generated sequence numbers will start at a given value and will not skip any numbers.

Here are the parameters:
SequenceGenerator <input> <output> <startingSeq> <numberOfReducers> <delimiter>

Here is an example of parameters:
SequenceGenerator ./input ./output 100 2 ,