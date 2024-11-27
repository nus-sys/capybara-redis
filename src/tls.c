/*
 * Copyright (c) 2019, Redis Labs
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "connhelpers.h"

#define TLS_AMALGAMATION
#include "tlse/tlse.c"

#define READ_BUF_SIZE 4096

/* The connections module provides a lean abstraction of network connections
 * to avoid direct socket and async event management across the Redis code base.
 *
 * It does NOT provide advanced connection features commonly found in similar
 * libraries such as complete in/out buffer management, throttling, etc. These
 * functions remain in networking.c.
 *
 * The primary goal is to allow transparent handling of TCP and TLS based
 * connections. To do so, connections have the following properties:
 *
 * 1. A connection may live before its corresponding socket exists.  This
 *    allows various context and configuration setting to be handled before
 *    establishing the actual connection.
 * 2. The caller may register/unregister logical read/write handlers to be
 *    called when the connection has data to read from/can accept writes.
 *    These logical handlers may or may not correspond to actual AE events,
 *    depending on the implementation (for TCP they are; for TLS they aren't).
 */

static ConnectionType CT_Socket;

/* When a connection is created we must know its type already, but the
 * underlying socket may or may not exist:
 *
 * - For accepted connections, it exists as we do not model the listen/accept
 *   part; So caller calls connCreateSocket() followed by connAccept().
 * - For outgoing connections, the socket is created by the connection module
 *   itself; So caller calls connCreateSocket() followed by connConnect(),
 *   which registers a connect callback that fires on connected/error state
 *   (and after any transport level handshake was done).
 *
 * NOTE: An earlier version relied on connections being part of other structs
 * and not independently allocated. This could lead to further optimizations
 * like using container_of(), etc.  However it was discontinued in favor of
 * this approach for these reasons:
 *
 * 1. In some cases conns are created/handled outside the context of the
 * containing struct, in which case it gets a bit awkward to copy them.
 * 2. Future implementations may wish to allocate arbitrary data for the
 * connection.
 * 3. The container_of() approach is anyway risky because connections may
 * be embedded in different structs, not just client.
 */


int read_from_file(const char *fname, void *buf, int max_len) {
    FILE *f = fopen(fname, "rb");
    if (f) {
        int size = fread(buf, 1, max_len - 1, f);
        if (size > 0)
            ((unsigned char *)buf)[size] = 0;
        else
            ((unsigned char *)buf)[0] = 0;
        fclose(f);
        return size;
    }
    return 0;
}

void load_keys(struct TLSContext *context, char *fname, char *priv_fname) {
    unsigned char buf[0xFFFF];
    unsigned char buf2[0xFFFF];
    int size = read_from_file(fname, buf, 0xFFFF);
    int size2 = read_from_file(priv_fname, buf2, 0xFFFF);
    if (size > 0 && context) {
        tls_load_certificates(context, buf, size);
        tls_load_private_key(context, buf2, size2);
        // tls_print_certificate(fname);
    }
}

struct TLSContext *server_context;

#define MAX_CONNECTIONS 64
struct ContextEntry {
    int fd;
    struct TLSContext *context;
} context_table[MAX_CONNECTIONS];


static int socketConfigure(void *privdata, int reconfigure) {
    UNUSED(privdata);
    UNUSED(reconfigure);
    server_context = tls_create_context(1, TLS_V12);
    load_keys(
        server_context, "/usr/local/tls/svr.crt", "/usr/local/tls/svr.key");
    serverLog(LL_VERBOSE,"TLS server context ready %p", (void *)server_context);

    for (int i = 0; i < MAX_CONNECTIONS; i += 1) {
        context_table[i].fd = -1;
        context_table[i].context = NULL;
    }
    return C_OK;
}

static connection *connCreateSocket(void) {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_Socket;
    conn->fd = -1;
    conn->iovcnt = IOV_MAX;

    return conn;
}

/* Create a new socket-type connection that is already associated with
 * an accepted connection.
 *
 * The socket is not ready for I/O until connAccept() was called and
 * invoked the connection-level accept handler.
 *
 * Callers should use connGetState() and verify the created connection
 * is not in an error state (which is not possible for a socket connection,
 * but could but possible with other protocols).
 */
static connection *connCreateAcceptedSocket(int fd, void *priv) {
    serverLog(LL_VERBOSE, "TLS connCreateAcceptedSocket (fd=%d)", fd);

    UNUSED(priv);
    connection *conn = connCreateSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;

    struct ContextEntry *entry = NULL;
    for (int i = 0; i < MAX_CONNECTIONS; i += 1) {
        if (context_table[i].fd == -1) {
        serverLog(LL_VERBOSE,"Allocate context table slot %d", i);
            entry = &context_table[i];
            break;
        }
    }
    if (!entry) {
        serverLog(LL_WARNING,"Context table exhausted");
        return NULL;
    }
    
    entry->fd = fd;
    entry->context = NULL;
    return conn;
}

static int connSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
        ConnectionCallbackFunc connect_handler) {
    serverLog(LL_WARNING,"Client side TLS is not implemented");

    int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTING;

    conn->conn_handler = connect_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
            conn->type->ae_handler, conn);

    return C_OK;
}

/* ------ Pure socket connections ------- */

/* A very incomplete list of implementation-specific calls.  Much of the above shall
 * move here as we implement additional connection types.
 */

static void connSocketShutdown(connection *conn) {
    if (conn->fd == -1) return;

    shutdown(conn->fd, SHUT_RDWR);
}

struct ContextEntry *find_context_entry(int fd) {
    struct ContextEntry *entry = NULL;
    for (int i = 0; i < MAX_CONNECTIONS; i += 1) {
        if (context_table[i].fd == fd) {
            entry = &context_table[i];
            break;
        }
    }
    if (!entry) {
        serverLog(LL_WARNING,"TLS context not found for fd = %d", fd);
    }
    return entry;
}

/* Close the connection and free resources. */
static void connSocketClose(connection *conn) {
    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        close(conn->fd);

        struct ContextEntry *entry = find_context_entry(conn->fd);
        entry->fd = -1;
        tls_destroy_context(entry->context);
        entry->context = NULL;

        conn->fd = -1;
    }

    /* If called from within a handler, schedule the close but
     * keep the connection until the handler returns.
     */
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }

    zfree(conn);
}

static int connSocketWrite(connection *conn, const void *data, size_t data_len) {
    serverLog(LL_VERBOSE, "TLS connSocketWrite, fd=%d, data_len=%zd", conn->fd, data_len);

    struct TLSContext *context = find_context_entry(conn->fd)->context;
    tls_write(context, data, data_len);
    unsigned int buf_len;
    const unsigned char *buf = tls_get_write_buffer(context, &buf_len);
    int ret = write(conn->fd, (void *)buf, buf_len);
    if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* Don't overwrite the state of a connection that is not already
         * connected, not to mess with handler callbacks.
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    if (ret != (int)buf_len) {
        serverLog(LL_WARNING,"TLS write incomplete");    
    }
    tls_buffer_clear(context);

    return data_len;
}

static int connSocketWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    serverLog(LL_VERBOSE, "TLS connSocketWritev, fd=%d", conn->fd);

    struct TLSContext *context = find_context_entry(conn->fd)->context;
    for (int i = 0; i < iovcnt; i += 1) {
        tls_write(context, iov[i].iov_base, iov[i].iov_len);
    }
    unsigned int buf_len;
    const unsigned char *buf = tls_get_write_buffer(context, &buf_len);
    int ret = write(conn->fd, (void *)buf, buf_len);
    if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* Don't overwrite the state of a connection that is not already
         * connected, not to mess with handler callbacks.
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    if (ret != (int)buf_len) {
        serverLog(LL_WARNING,"TLS write incomplete");    
    }
    tls_buffer_clear(context);

    return ret;
}

static int connSocketRead(connection *conn, void *buf, size_t buf_len) {
    unsigned char read_buf[READ_BUF_SIZE];
    int ret = read(conn->fd, read_buf, READ_BUF_SIZE);
    if (!ret) {
        conn->state = CONN_STATE_CLOSED;
    } else if (ret < 0 && errno != EAGAIN) {
        conn->last_errno = errno;

        /* Don't overwrite the state of a connection that is not already
         * connected, not to mess with handler callbacks.
         */
        if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
            conn->state = CONN_STATE_ERROR;
    }

    struct TLSContext *context = find_context_entry(conn->fd)->context;
    tls_consume_stream(context, read_buf, ret, NULL);
    int tls_ret = tls_read(context, buf, buf_len);
    serverLog(LL_VERBOSE, "TLS connSocketRead, fd=%d, read(..)=%d tls_read(..)=%d", conn->fd, ret, tls_ret);
    if (tls_ret != 0) {
        return tls_ret;
    }
    return ret;
}

static void connSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask);

static int connSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    serverLog(LL_VERBOSE, "TLS connSocketAccept fd=%d", conn->fd);
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;

    struct ContextEntry *entry = find_context_entry(conn->fd);
    entry->context = tls_accept(server_context);
    conn->conn_handler = accept_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_READABLE,connSocketEventHandler, conn);
    return ret;
}

/* Register a write handler, to be called when the connection is writable.
 * If NULL, the existing handler is removed.
 *
 * The barrier flag indicates a write barrier is requested, resulting with
 * CONN_FLAG_WRITE_BARRIER set. This will ensure that the write handler is
 * always called before and not after the read handler in a single event
 * loop.
 */
static int connSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    if (func == conn->write_handler) return C_OK;

    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    if (!conn->write_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,AE_WRITABLE,
                    conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

/* Register a read handler, to be called when the connection is readable.
 * If NULL, the existing handler is removed.
 */
static int connSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

static const char *connSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static void connSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask)
{
    UNUSED(el);
    connection *conn = clientData;

    serverLog(LL_VERBOSE, "TLS connSocketEventHandler(): fd=%d, state=%d, mask=%d, r=%d, w=%d, flags=%d",
        fd, conn->state, mask, conn->read_handler != NULL, conn->write_handler != NULL, conn->flags);

    if (conn->state == CONN_STATE_CONNECTING) {
        serverLog(LL_WARNING, "TLS is not implemented for CONN_STATE_CONNECTING");
    }

    if (conn->state == CONN_STATE_ACCEPTING) {
        struct TLSContext *context = find_context_entry(fd)->context;
        if (mask & AE_READABLE) {
            unsigned char read_buf[READ_BUF_SIZE];
            while (1) {
                int ret = read(fd, read_buf, READ_BUF_SIZE);
                if (ret == 0) {
                    serverLog(LL_WARNING, "TLS connection closed during handshake");
                    conn->state = CONN_STATE_ERROR;
                    return;
                }
                if (ret < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    conn->last_errno = errno;
                    conn->state = CONN_STATE_ERROR;
                    return;
                }
                serverLog(LL_VERBOSE, "TLS read, length %d", ret);
                tls_consume_stream(context, read_buf, ret, NULL);
            }
        }

        unsigned int write_len;
        const unsigned char *write_buf = tls_get_write_buffer(context, &write_len);
        if (write_len > 0) {
            int ret = write(fd, write_buf, write_len);
            if (ret < 0 && !(errno == EAGAIN || errno == EWOULDBLOCK)) {
                conn->last_errno = errno;
                conn->state = CONN_STATE_ERROR;
                return;
            }
            if (ret < (int)write_len) {
                serverLog(LL_WARNING, "TLS incomplete write");
            }
            serverLog(LL_VERBOSE, "TLS write, length %d", ret);
            tls_buffer_clear(context);
        }

        if (tls_established(context)) {
            serverLog(LL_VERBOSE, "TLS established, cipher=%s", tls_cipher_name(context));
            conn->state = CONN_STATE_CONNECTED;
            if (!callHandler((connection *) conn, conn->conn_handler)) return;
            conn->conn_handler = NULL;
        } else {
            aeCreateFileEvent(server.el, conn->fd, AE_READABLE,connSocketEventHandler, conn);
            return;
        }
    }

    if (conn->state == CONN_STATE_CONNECTING &&
            (mask & AE_WRITABLE) && conn->conn_handler) {

        int conn_error = anetGetError(conn->fd);
        if (conn_error) {
            conn->last_errno = conn_error;
            conn->state = CONN_STATE_ERROR;
        } else {
            conn->state = CONN_STATE_CONNECTED;
        }

        if (!conn->write_handler) aeDeleteFileEvent(server.el,conn->fd,AE_WRITABLE);

        if (!callHandler(conn, conn->conn_handler)) return;
        conn->conn_handler = NULL;
    }

    /* Normally we execute the readable event first, and the writable
     * event later. This is useful as sometimes we may be able
     * to serve the reply of a query immediately after processing the
     * query.
     *
     * However if WRITE_BARRIER is set in the mask, our application is
     * asking us to do the reverse: never fire the writable event
     * after the readable. In such a case, we invert the calls.
     * This is useful when, for instance, we want to do things
     * in the beforeSleep() hook, like fsync'ing a file to disk,
     * before replying to a client. */
    int invert = conn->flags & CONN_FLAG_WRITE_BARRIER;

    int call_write = (mask & AE_WRITABLE) && conn->write_handler;
    int call_read = (mask & AE_READABLE) && conn->read_handler;

    /* Handle normal I/O flows */
    if (!invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
    /* Fire the writable event. */
    if (call_write) {
        if (!callHandler(conn, conn->write_handler)) return;
    }
    /* If we have to invert the call, fire the readable event now
     * after the writable one. */
    if (invert && call_read) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

static void connSocketAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        acceptCommonHandler(connCreateAcceptedSocket(cfd, NULL),0,cip);
    }
}

static int connSocketAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    if (anetFdToString(conn->fd, ip, ip_len, port, remote) == 0)
        return C_OK;

    conn->last_errno = errno;
    return C_ERR;
}

static int connSocketIsLocal(connection *conn) {
    char cip[NET_IP_STR_LEN + 1] = { 0 };

    if (connSocketAddr(conn, cip, sizeof(cip) - 1, NULL, 1) == C_ERR)
        return -1;

    return !strncmp(cip, "127.", 4) || !strcmp(cip, "::1");
}

static int connSocketListen(connListener *listener) {
    return listenToPort(listener);
}

static int connSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
    serverLog(LL_WARNING,"TLS is not implemented for blocking connect");
    int fd = anetTcpNonBlockConnect(NULL,addr,port);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    if ((aeWait(fd, AE_WRITABLE, timeout) & AE_WRITABLE) == 0) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = ETIMEDOUT;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTED;
    return C_OK;
}

/* Connection-based versions of syncio.c functions.
 * NOTE: This should ideally be refactored out in favor of pure async work.
 */

static ssize_t connSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    serverLog(LL_WARNING,"TLS is not implemented for sync write");
    return syncWrite(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    serverLog(LL_WARNING,"TLS is not implemented for sync read");
    return syncRead(conn->fd, ptr, size, timeout);
}

static ssize_t connSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    serverLog(LL_WARNING,"TLS is not implemented for sync read line");
    return syncReadLine(conn->fd, ptr, size, timeout);
}

static const char *connSocketGetType(connection *conn) {
    (void) conn;

    return CONN_TYPE_TLS;
}

static ConnectionType CT_Socket = {
    /* connection type */
    .get_type = connSocketGetType,

    /* connection type initialize & finalize & configure */
    .init = NULL,
    .cleanup = NULL,
    .configure = socketConfigure,

    /* ae & accept & listen & error & address handler */
    .ae_handler = connSocketEventHandler,
    .accept_handler = connSocketAcceptHandler,
    .addr = connSocketAddr,
    .is_local = connSocketIsLocal,
    .listen = connSocketListen,

    /* create/shutdown/close connection */
    .conn_create = connCreateSocket,
    .conn_create_accepted = connCreateAcceptedSocket,
    .shutdown = connSocketShutdown,
    .close = connSocketClose,

    /* connect & accept */
    .connect = connSocketConnect,
    .blocking_connect = connSocketBlockingConnect,
    .accept = connSocketAccept,

    /* IO */
    .write = connSocketWrite,
    .writev = connSocketWritev,
    .read = connSocketRead,
    .set_write_handler = connSocketSetWriteHandler,
    .set_read_handler = connSocketSetReadHandler,
    .get_last_error = connSocketGetLastError,
    .sync_write = connSocketSyncWrite,
    .sync_read = connSocketSyncRead,
    .sync_readline = connSocketSyncReadLine,

    /* pending data */
    .has_pending_data = NULL,
    .process_pending_data = NULL,
};

int RedisRegisterConnectionTypeTLS(void)
{
    return connTypeRegister(&CT_Socket);
}
