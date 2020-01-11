# Project-2: Log-structured file I/O

### Part 1: Error handling

### Part 2: How the log behaves

### Part 3: Writing to the log

#### Q1: Do you want to prioritize performance? 
`No, I think consider it too early.`

#### Q2: Do you want to be able to read the content of the log in plain text?
`Yes, I think it might be simpler to use plain text.`

#### Q4: Where is the system performing buffering and where do you need buffering? 
`I think I should have buffer when I read and write the log file.`

#### Q6: What is the impact of buffering on subsequent reads? 
`I think the buffer may help us complete the index.`

#### Q7: When should you open and close file handles? For each command? For the lifetime of the KvStore?
`I think lifetime of the KvStore maybe a better choice.`

### Part 4: Reading from the log

### Part 5: Storing log pointers in the index

### Part 6: Stateless vs. stateful KvStore 
`My database is stateful database.`

### Part 7: Compacting the log

#### Q1: What is the naive solution? 
`Read the all logs and remove it.`

#### Q2: How much memory do you need?
`The number of keys.`

#### Q3: What is the minimum amount of copying necessary to compact the log?
`The number of keys.`

#### Q4: Can the compaction be done in-place?
`No, I think I have to copy the logs.`

#### Q4: How do you maintain data-integrity if compaction fails?
`Maybe we can make a copy before we start compact.`











