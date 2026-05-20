//
// Created by zwx on 4/2/26.
//

#include "server.h"

#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <stdexcept>
#include <thread>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

namespace server
{
    int create_listen_socket(const std::string &ip, int port, int backlog) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket() failed");

        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
            ::close(fd);
            throw std::runtime_error("inet_pton() failed");
        }

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(fd);
            throw std::runtime_error("bind() failed");
        }

        if (::listen(fd, backlog) < 0) {
            ::close(fd);
            throw std::runtime_error("listen() failed");
        }

        return fd;
    }

    std::string read_line(int fd, bool &ok) {
        ok = false;
        std::string out;
        char c;
        while (true) {
            ssize_t n = ::recv(fd, &c, 1, 0);
            if (n == 0) return ""; // peer closed
            if (n < 0) {
                if (errno == EINTR) continue;
                return "";
            }
            if (c == '\n') {
                ok = true;
                return out;
            }
            if (c != '\r') out.push_back(c);
            // Cross-domain VitaBench projections can exceed 64KB per command.
            // Keep a defensive cap, but raise it enough for real benchmark payloads.
            if (out.size() > 1024 * 1024) return "";
        }
    }

    bool send_all(int fd, const std::string &data) {
        size_t sent = 0;
        while (sent < data.size()) {
            ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            sent += static_cast<size_t>(n);
        }
        return true;
    }

    void run_accept_loop(int listen_fd, const ClientHandler &handler) {
        while (true) {
            sockaddr_in cli{};
            socklen_t len = sizeof(cli);
            int cfd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&cli), &len);
            if (cfd < 0) {
                if (errno == EINTR) continue;
                continue;
            }

            std::thread([cfd, handler]() {
                handler(cfd);
                ::close(cfd);
            }).detach();
        }
    }
}
