//
// Created by zwx on 4/2/26.
//

#ifndef SERVER_H
#define SERVER_H

#include <string>
#include <functional>

namespace server
{
    using ClientHandler = std::function<void(int)>;

    int create_listen_socket(const std::string &ip, int port, int backlog = 128);
    std::string read_line(int fd, bool &ok);
    bool send_all(int fd, const std::string &data);
    void run_accept_loop(int listen_fd, const ClientHandler &handler);
}


#endif //SERVER_H
