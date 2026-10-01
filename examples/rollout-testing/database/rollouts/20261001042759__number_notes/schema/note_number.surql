-- Notes are numbered; a sequence hands out the numbers.
DEFINE SEQUENCE note_no BATCH 1 START 1;
DEFINE FIELD number ON note TYPE int DEFAULT sequence::nextval('note_no') READONLY;
