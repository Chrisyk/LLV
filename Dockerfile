FROM gcc:latest

WORKDIR /app

COPY . .

RUN apt-get update && apt-get install -y cmake

RUN rm -rf CMakeCache.txt CMakeFiles

RUN cmake . && make

# Run the microbenchmark by default
CMD ["./bench_microbenchmark"]