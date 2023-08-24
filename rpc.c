#include "rpc.h"
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <endian.h>

#define NONBLOCKING
#define RPC_FIND 3
#define RPC_CALL 2
#define RPC_SUCCESS 1
#define RPC_FAILURE 0

typedef struct {
    // will be passed into the new thread as an argument
    int socket;
    rpc_server *srv;
} input;

struct rpc_handle {
    char name[1000];
    rpc_data *(*handler)(rpc_data *);
};

struct rpc_server {
    int port;
    int socket;
    int numHandlers;
    rpc_handle *handlers[11];
};



rpc_server *rpc_init_server(int port) {
    // Check that the port is valid
    if ((port < 0) || (port > 65535)) {
        fprintf(stderr, "invalid port number : %d\n", port);
        return NULL;
    }
    int sockfd, s, re;
    rpc_server* server;
    char str_port[10];
    sprintf(str_port, "%d", port);
    struct addrinfo hints, *res;
    server = (rpc_server *) malloc(sizeof(rpc_server));
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET6;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    s = getaddrinfo(NULL, str_port, &hints, &res);
    if (s != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        return NULL;
	}
    // Create listening socket
    sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    // Reuse port if possible
	re = 1;
	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &re, sizeof(int)) < 0) {
		perror("setsockopt");
        exit(EXIT_FAILURE);
	}
    // Bind address to the socket
	if (bind(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
		perror("bind");
	}
    // Listen
    if (listen(sockfd, 10) < 0) {
		perror("listen");
    }
    server->socket = sockfd;
    server->port = port;
    server->numHandlers = 0;
	freeaddrinfo(res);
    return server;
}

int rpc_register(rpc_server *srv, char *name, rpc_handler handler) {
    // Check whether handle already exists
    for (int i = 0; i < srv->numHandlers; i++) {
        if (strcmp(srv->handlers[i]->name, name) == 0) {
            srv->handlers[i]->handler = handler;
            return 0;
        }
    }
    // Create new handle
    rpc_handle *new = (rpc_handle *) malloc (sizeof(rpc_handle));
    srv->handlers[srv->numHandlers] = new;
    strcpy(srv->handlers[srv->numHandlers]->name, name);
    srv->handlers[srv->numHandlers]->handler = handler;
    srv->numHandlers++;
    if (new != NULL) {
        return 0;
    }
    return -1;
}

void *request_rpc(void *new_connection) {
    ssize_t bytes_written, bytes_read;
    input *in = (input *) new_connection;
    int newsocket = in->socket;
    rpc_server *srv = in->srv;
    // Check which function is called
    uint64_t function_check;
    uint64_t function_call;
    while (read(newsocket, &function_check, sizeof(function_check)) > 0) {
        function_call = be64toh(function_check);
        char name_buffer[1000];
        // if rpc_find is called
        if (function_call == RPC_FIND) {
            int success = RPC_FAILURE;
            // Read characters from the connection
		    bytes_read = read(newsocket, name_buffer, sizeof(name_buffer)); 
            if (bytes_read < 0) {
                perror("read");
            }
            name_buffer[bytes_read] = '\0';
            // Find handle, and send name if found
            for (int i = 0; i < srv->numHandlers; i++) {
                if (strcmp(srv->handlers[i]->name, name_buffer) == 0) {
                    success = RPC_SUCCESS;
                    bytes_written = write(newsocket, &success, sizeof(success));
                    if (bytes_written < 0) {
                        perror("write");
                    }
                    bytes_written = write(newsocket, srv->handlers[i]->name, strlen(srv->handlers[i]->name) + 1);
                    if (bytes_written < 0) {
                        perror("write");
                    }
                }
            }
            // Send success code
            if (success == RPC_FAILURE) {
                bytes_written = write(newsocket, &success, sizeof(success));
                if (bytes_written < 0) {
                    perror("write");
                }
            }
        } 
        // if rpc_call is called
        else if (function_call == RPC_CALL) {
            char handle_name[1000];
            rpc_data *data = (rpc_data *) malloc(sizeof(rpc_data));
            // Read data 
            uint64_t received_data1;
            uint64_t received_data2_len;
            bytes_read = read(newsocket, &(received_data1), sizeof(received_data1));
            if (bytes_read < 0) {
                perror("read");
            }
            received_data1 = be64toh(received_data1);
            data->data1 = (int)received_data1;
            bytes_read = read(newsocket, &(received_data2_len), sizeof(received_data2_len));
            if (bytes_read < 0) {
                perror("read");
            }
            received_data2_len = be64toh(received_data2_len);
            data->data2_len = received_data2_len;
            if (data->data2_len > 0) {
                data->data2 = malloc(data->data2_len);
                bytes_read = read(newsocket, data->data2, data->data2_len);
                if (bytes_read < 0) {
                    perror("read");
                }
            }
            // Read the handle name from the connection
            bytes_read = read(newsocket, handle_name, sizeof(handle_name)); 
            if (bytes_read < 0) {
                perror("read");
            }
            handle_name[bytes_read] = '\0';
   
            rpc_data *response = (rpc_data *) malloc(sizeof(rpc_data));
            // If handle exists, pass data and get response
            for (int i = 0; i < srv->numHandlers; i++) {
                if (strcmp(srv->handlers[i]->name, handle_name) == 0) {
                    response = srv->handlers[i]->handler(data);
                    break;
                }
            }
            // Check if data is valid. Sends RPC_SUCCESS if valid, RPC_FAILURE otherwise
            int success = RPC_FAILURE;
            if (response == NULL) {
                bytes_written = write(newsocket, &success, sizeof(success));
                if (bytes_written < 0) {
                    perror("write");
                }
                rpc_data_free(data);
                rpc_data_free(response);
                continue;
            } else if ((response->data2_len > 0) && (response->data2 == NULL)) {
                bytes_written = write(newsocket, &success, sizeof(success));
                if (bytes_written < 0) {
                    perror("write");
                }
                rpc_data_free(data);
                rpc_data_free(response);
                continue;
            } else if ((response->data2_len == 0) && (response->data2 != NULL)) {
                bytes_written = write(newsocket, &success, sizeof(success));
                if (bytes_written < 0) {
                    perror("write");
                }
                rpc_data_free(data);
                rpc_data_free(response);
                continue;
            } else {
                success = RPC_SUCCESS;
                bytes_written = write(newsocket, &success, sizeof(success));
                if (bytes_written < 0) {
                    perror("write");
                }
            }

            // // Send the data over in a single write to avoid TCP issues
            uint64_t response_data1 = response->data1;
            uint64_t response_data2_len = response->data2_len;
            response_data1 = htobe64(response_data1);
            response_data2_len = htobe64(response_data2_len);
            char* buffer;
            size_t total_size = sizeof(response_data1) + sizeof(response_data2_len) 
            + response->data2_len;
            buffer = (char*)malloc(total_size);
            char* current = buffer;
            memcpy(current, &(response_data1), sizeof(response_data1));
            current += sizeof(response_data1);
            memcpy(current, &(response_data2_len), sizeof(response_data2_len));
            current += sizeof(response_data2_len);
            memcpy(current, response->data2, response->data2_len);
            bytes_written = write(newsocket, buffer, total_size);
            if (bytes_written < 0) {
                perror("write");
            }

            free(buffer);
            rpc_data_free(data);
            rpc_data_free(response);
        }
    }
    return NULL;
}

void rpc_serve_all(rpc_server *srv) {
    struct sockaddr_storage client_addr;
	socklen_t client_addr_size = sizeof client_addr;
    while (1) {
        // Accept a new connection, and create a new thread to handle it
        int newsocket = accept(srv->socket, (struct sockaddr*)&client_addr, &client_addr_size);
        if (newsocket > 0) {
            input connection;
            connection.socket = newsocket;
            connection.srv = srv;
            pthread_t thread;
            if (pthread_create(&thread, NULL, request_rpc, &connection) == 0) {
                continue; 
            }
        }
    }
}

struct rpc_client {
    char *IPAddress;
    int port;
    int socket;
    int closed;
};

rpc_client *rpc_init_client(char *addr, int port) {
    rpc_client* client;
    client = (rpc_client *) malloc(sizeof(rpc_client));
    int sockfd, s;
    struct addrinfo hints, *servinfo, *rp;
	char str_port[10];
    sprintf(str_port, "%d", port);
    // Create address
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET6;
	hints.ai_socktype = SOCK_STREAM;
    // Get addrinfo of server
    s = getaddrinfo(addr, str_port, &hints, &servinfo);
	if (s != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
        return NULL;
	}
    // Connect to first valid result
    for (rp = servinfo; rp != NULL; rp = rp->ai_next) {
		sockfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sockfd == -1)
			continue;

		if (connect(sockfd, rp->ai_addr, rp->ai_addrlen) != -1)
			break; // success

		close(sockfd);
	}
	if (rp == NULL) {
		fprintf(stderr, "client: failed to connect\n");
        freeaddrinfo(servinfo);
        free(client);
        return NULL;
	}
    client->port = port;
    client->closed = 0;
    client->IPAddress = addr;
    client->socket = sockfd;
	freeaddrinfo(servinfo);
    return client;
}

rpc_handle *rpc_find(rpc_client *cl, char *name) {
    int status;
    char buffer[1000];
    ssize_t bytes_written, bytes_read;
    rpc_handle *handle = (rpc_handle *) malloc(sizeof(rpc_handle));
    // Send 0 to indicate we are calling rpc_find
    uint64_t function_call = RPC_FIND;
    uint64_t function = htobe64(function_call);
    bytes_written = write(cl->socket, &function, sizeof(function));
    if (bytes_written < 0) {
        perror("write");
    }

    // Send message to server
	bytes_written = write(cl->socket, name, strlen(name) + 1);
    if (bytes_written < 0) {
        perror("write");
    }
	
    // Read message from server
    bytes_read = read(cl->socket, &status, sizeof(status));
    if (bytes_read < 0) {
        perror("write");
    }
    if (status == RPC_SUCCESS) {
        bytes_read = read(cl->socket, buffer, sizeof(buffer));
        if (bytes_read < 0) {
            perror("write");
        }
        buffer[bytes_read] = '\0';
        strcpy(handle->name, buffer);

    } else if (status == RPC_FAILURE) {
        free(handle);
        return NULL;
    }
	
    return handle;
}

rpc_data *rpc_call(rpc_client *cl, rpc_handle *h, rpc_data *payload) {
    // check validity of the payload
    if (payload == NULL) {
        return NULL;
    } 
    if (payload->data2_len > 0) {
        if (payload->data2 == NULL) {
            return NULL;
        }
    }
    if (payload->data2_len == 0) {
        if (payload->data2 != NULL) {
            return NULL;
        }
    }
    rpc_data *result = (rpc_data *) malloc(sizeof(rpc_data));
    result->data2 = NULL;
    char* buffer;
    ssize_t bytes_written, bytes_read;
    // Send 1 to indicate we are calling rpc_call
    uint64_t function_call = RPC_CALL;
    uint64_t function = htobe64(function_call);
    bytes_written = write(cl->socket, &function, sizeof(function));
    if (bytes_written < 0) {
        perror("write");
    }

    // Send the data over in a single write to avoid TCP issues
    uint64_t new_data1 = payload->data1;
    new_data1 = htobe64(new_data1);
    uint64_t new_data2_len = payload->data2_len;
    new_data2_len = htobe64(new_data2_len);
    size_t total_size = sizeof(new_data1) + sizeof(new_data2_len) 
    + payload->data2_len;
    buffer = (char*)malloc(total_size);
    char* current = buffer;
    memcpy(current, &(new_data1), sizeof(new_data1));
    current += sizeof(new_data1);
    memcpy(current, &(new_data2_len), sizeof(new_data2_len));
    current += sizeof(new_data2_len);
    memcpy(current, payload->data2, payload->data2_len);

    bytes_written = write(cl->socket, buffer, total_size);
    if (bytes_written < 0) {
        perror("write");
    }
    
    free(buffer);

    // Send handle
    bytes_written = write(cl->socket, h->name, strlen(h->name) + 1);
    if (bytes_written < 0) {
        perror("write");
    }

    // Make sure we get a valid response
    int status;
    bytes_read = read(cl->socket, &status, sizeof(status));
    if (bytes_read < 0) {
        perror("write");
    }
    if (status == RPC_FAILURE) {
        free(result);
        return NULL;
    }

    // Read the response 
    uint64_t result_data1;
    uint64_t result_data2_len;
	bytes_read = read(cl->socket, &(result_data1), sizeof(result_data1));
    if (bytes_read < 0) {
        perror("write");
    }
    result_data1 = be64toh(result_data1);
    result->data1 = (int)result_data1;
    bytes_read = read(cl->socket, &(result_data2_len), sizeof(result_data2_len));
     if (bytes_read < 0) {
        perror("write");
    }
    result_data2_len = be64toh(result_data2_len);
    result->data2_len = result_data2_len;
    if (result->data2_len > 0) {
        result->data2 = malloc(result->data2_len);
        bytes_read = read(cl->socket, result->data2, result->data2_len);
        if (bytes_read < 0) {
            perror("write");
        }
    }
    return result;
}

void rpc_close_client(rpc_client *cl) {
    // free
    cl->closed = 1;
    close(cl->socket);
    free(cl);
}

void rpc_data_free(rpc_data *data) {
    if (data == NULL) {
        return;
    }
    if (data->data2 != NULL) {
        free(data->data2);
    }
    free(data);
}