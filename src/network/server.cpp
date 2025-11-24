#include "server.h"
#include <iostream>

Server::Server(int port) : port_(port) {}

void Server::start() {
    std::cout << "Server starting on port " << port_ << "\n";
}

void Server::stop() {
    std::cout << "Server stopping\n";
}
