#ifndef SERVER_H
#define SERVER_H

#include <string>

class Server {
public:
    Server(int port);
    void start();
    void stop();
private:
    int port_;
};

#endif
