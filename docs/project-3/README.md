# Project-3: Synchronous client-server networking

### Part 1: Command line parsing

### Part 2: Logging

### Part 3: Client-server networking setup

### Part 4: Implementing commands across the network

### Part 5: Pluggable storage engines

### Part 6: Benchmarking

#### Q1: What code should be timed (and be written inside the benchmark loop), and what code should not (and be written outside the benchmark loop)?
`I think the init code should not be timed.`

#### Q2: How to make the loop run identically for each iteration, despite using "random" numbers?
`Use same value.`

#### Q3: In the "read" benchmarks, how to read from the same set of "random" keys that were written previously?
`Use same seed for StdRng.`




