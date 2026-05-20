#include <iostream>
#include <cstdlib>
#include "Server/server.h"
#include "Server/session.h"
#include "Server/kv_api.h"

int main()
{

    if (!server::init_engine()) {
        std::cerr << "engine init failed\n";
        return 1;
    }

    int port = 19091;
    if (const char *env_port = std::getenv("TREE_DB_PORT")) {
        try {
            port = std::stoi(env_port);
        } catch (...) {
            port = 19091;
        }
    }

    int listen_fd = server::create_listen_socket("0.0.0.0", port, 128);
    std::cout << "DataAgentDB listening on 0.0.0.0:" << port << "\n";

    ///todo: thread pool
    server::run_accept_loop(listen_fd, [](int client_fd) {
        server::handle_client(client_fd);
    });

    return 0;
}

/*
 connect: nc 127.0.0.1 19091

*/
