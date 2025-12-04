# LLV

## Compiling and Running

### Option 1: Docker (Recommended)

This is the simplest way to run the benchmark in a controlled environment.

1.  **Build and Run the Docker image:**
    ```bash
    NUM_THREADS=5 DURATION=10 docker-compose up --build
    ```

### Option 2: CMake (Local Build)

If you prefer to build and run directly on your machine:

1.  **Create a build directory:**
    ```bash
    mkdir -p build && cd build
    ```

2.  **Configure the project:**
    ```bash
    cmake ..
    ```

3.  **Build the executable:**
    ```bash
    make
    ```

4.  **Run the benchmark:**
    ```bash
    ./bench_microbenchmark
    ```

## Project Overview

This project benchmarks two concurrency control protocols:
*   **VLL (Very-Long-Lived Locking):** Uses a deterministic transaction queue and localized metadata to manage concurrency without a central lock manager.
*   **2PL (Two-Phase Locking):** A standard baseline implementation using atomic lock acquisition to prevent deadlocks.

## Directory Structure

*   `bench/`: Microbenchmark driver and workload configuration.
*   `src/concurrency/`: Implementations of VLL and 2PL.
*   `src/core/`: Storage manager and record definitions.
*   `src/transaction/`: Transaction structure and definitions.

