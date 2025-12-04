FROM gcc:latest

WORKDIR /app

COPY . .

# Install build dependencies and Python for plotting
RUN apt-get update && apt-get install -y cmake python3 python3-matplotlib python3-numpy dos2unix

# Build the benchmark
RUN mkdir -p build && cd build && cmake .. && make

# Make scripts executable
RUN chmod +x /app/scripts/*.sh /app/scripts/*.py

# Fix line endings
RUN dos2unix /app/scripts/*.sh /app/scripts/*.py

# Default: run the benchmark script
CMD ["/bin/bash", "/app/scripts/run_benchmark.sh"]
