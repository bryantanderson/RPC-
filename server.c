#include "rpc.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

rpc_data *add2_i8(rpc_data *);

int main(int argc, char *argv[]) {
    int port;
    rpc_server *state;
    port = atoi(argv[2]);
    state = rpc_init_server(port);
    if (state == NULL) {
        fprintf(stderr, "Failed to init\n");
        exit(EXIT_FAILURE);
    }

    if (rpc_register(state, "add2", add2_i8) == -1) {
        fprintf(stderr, "Failed to register add2\n");
        exit(EXIT_FAILURE);
    }

    rpc_serve_all(state);

    return 0;
}

/* Adds 2 signed 8 bit numbers */
/* Uses data1 for left operand, data2 for right operand */
rpc_data *add2_i8(rpc_data *in) {
    /* Check data2 */
    if (in->data2 == NULL || in->data2_len != 1) {
        return NULL;
    }

    /* Parse request */
    char n1 = in->data1;
    char n2 = ((char *)in->data2)[0];

    /* Perform calculation */
    printf("add2: arguments %d and %d\n", n1, n2);
    int res = n1 + n2;
    /* Prepare response */
    rpc_data *out = malloc(sizeof(rpc_data));
    assert(out != NULL);
    out->data1 = res;
    out->data2_len = 0;
    out->data2 = NULL;
    return out;
}
