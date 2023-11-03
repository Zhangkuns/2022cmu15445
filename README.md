# 2022cmu15445
personal solution for the 2022fall cmu15445
# CMU 15-445/645 Database Systems (Fall 2022) Assignments

This repository contains my personal solutions for the assignments of the CMU 15-445/645 Database Systems course. The solutions are provided for educational purposes and to facilitate discussion and learning among peers taking the course.

## About the Course

CMU 15-445/645 is an introduction to database systems. The course covers various topics including storage and indexing, query execution and optimization, transaction management, and more.

## Assignments

The assignments in this course are designed to provide hands-on experience with the implementation of core database system components.

### Assignment 1: Buffer Pool Manager

- **Objective:** Implement a buffer pool manager that handles page fetching and eviction.
- **Key Concepts:** LRU caching, page replacement, concurrency control.

### Assignment 2: B+ Tree Index

- **Objective:** Build a B+ tree index that supports range queries and updates.
- **Key Concepts:** Tree data structures, index locking, key insertion and deletion.

### Assignment 3: Query Execution

- **Objective:** Develop query execution operators like `SELECT`, `JOIN`, and `AGGREGATE`.
- **Key Concepts:** Query planning, operator trees, iterator model.

### Assignment 4: Concurrency Control

- **Objective:** Implement concurrency control mechanisms to handle transaction isolation.
- **Key Concepts:** Locking protocols, deadlock prevention, multi-version concurrency control.

## Usage

The solutions are organized by assignment. To run a specific assignment's tests, navigate to the assignment directory and follow the instructions provided in the corresponding README.

```bash
cd assignment1
make test
