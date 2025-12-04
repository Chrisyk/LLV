FROM gcc:latest

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y cmake

RUN mkdir build && cd build && cmake .. && make

# Run the microbenchmark by default
CMD ["./build/bench_microbenchmark"]
