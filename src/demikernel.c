/*
 * Copyright (c) 2022, Microsoft Corporation
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
#include <demi/sga.h>
#include <demi/libos.h>
#include <demi/types.h>
#include <demi/wait.h>
#include <arpa/inet.h>

static ConnectionType CT_DemiSocket;

connection *connCreateDemikernelSocket(void) {
    connection *conn = zcalloc(sizeof(connection));
    conn->type = &CT_DemiSocket;
    conn->fd = -1;
    conn->iovcnt = IOV_MAX;
    conn->private_data = zcalloc(sizeof(demi_qresult_t));
    return conn;
}

connection *connCreateAcceptedDemiQ(int fd, void *priv) {
    UNUSED(priv);

    connection *conn = connCreateDemikernelSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_ACCEPTING;

    return conn;
}

connection *connCreateListeningSocket(int fd) {
    connection *conn = connCreateDemikernelSocket();
    conn->fd = fd;
    conn->state = CONN_STATE_LISTENING;
    return conn;
}

static int demiSocketConnect(connection *conn, const char *addr, int port, const char *src_addr,
                             ConnectionCallbackFunc connect_handler) {
    /* int fd = anetTcpNonBlockBestEffortBindConnect(NULL,addr,port,src_addr);
    if (fd == -1) {
        conn->state = CONN_STATE_ERROR;
        conn->last_errno = errno;
        return C_ERR;
    }

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTING;

    conn->conn_handler = connect_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
                      conn->type->ae_handler, conn); */

    UNUSED(src_addr);

    // TEMP
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = inet_addr(addr);

    int fd, retval;
    demi_qtoken_t qt;
    demi_qresult_t qr;
    fprintf(stderr, "demi_socket() = %d\n", retval = demi_socket(&fd, AF_INET, SOCK_STREAM, 0));
    if(retval) while(1);
    fprintf(stderr, "demi_connect() = %d\n", retval = demi_connect(&qt, fd, (struct sockaddr *) &address, sizeof(address)));
    if(retval) while(1);
    fprintf(stderr, "demi_wait() = %d\n", retval = demi_wait(&qr, qt));
    if(retval) while(1);
    fprintf(stderr, "[*] Connected to master at %s:%d\n", addr, port);

    conn->fd = fd;
    conn->state = CONN_STATE_CONNECTING;
    conn->conn_handler = connect_handler;
    aeCreateFileEvent(server.el, conn->fd, AE_WRITABLE,
                      conn->type->ae_handler, conn);
    return C_OK;
}

static void demiSocketClose(connection *conn) {
    if (conn->fd != -1) {
        aeDeleteFileEvent(server.el,conn->fd, AE_READABLE | AE_WRITABLE);
        close(conn->fd);
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

#ifdef __REDIS_LOG__
void redis_log_cmd(const void *buf, const size_t len, int tx) {
    if(tx) fprintf(stderr, ">> ");
    for(size_t i = 0; i < len; i++) {
        char c = ((char *) buf)[i];
        if(c == '\r') continue;
        if(c == '\n') c = ' ';
        fputc(c, stderr);
    }
}

/// With line feed.
void redis_logn_cmd(const void *buf, const size_t len, int tx) {
    redis_log_cmd(buf, len, tx);
    fputc('\n', stderr);
}
#else
#define redis_log_cmd(...) do {} while(0)
#define redis_logn_cmd(...) do {} while(0)
#endif /* __REDIS_LOG__ */

static int demiSocketWrite(connection *conn, const void *data, size_t data_len) {
    demi_sgarray_t sga = demi_sgaalloc(data_len);
    demi_qtoken_t qt;
    // demi_qresult_t qr;
    int ret;

#ifdef __DEMIKERNEL_LOG_IO__
    eprintln_cmd(data, data_len, 1);
#endif /* __DEMIKERNEL_LOG_IO__ */

    memcpy(sga.sga_segs[0].sgaseg_buf, data, data_len);

    // if (((ret = demi_push(&qt, conn->fd, &sga)) != 0 ||
    //     (ret = demi_wait(&qr, qt)) != 0 ||
    //     qr.qr_opcode != DEMI_OPC_PUSH) &&
    //     ret != EAGAIN) {
    if(
        (ret = demi_push(&qt, conn->fd, &sga)) != 0 &&
        ret != EAGAIN
    ) {
        conn->last_errno = ret;
        fprintf(stderr, "demiSocketWrite error QD %d: %s\n", conn->fd, strerror(ret));
        return -1;
    }

    // Store push qtoken to be waited on later.
    push_qtoken(qt);

    redis_log("REDIS write cmd = ");
    redis_logn_cmd(data, data_len, 1);

    demi_sgafree(&sga);
    return data_len;
}

static int demiSocketWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    size_t data_len = 0;
    demi_sgarray_t sga;
    demi_qtoken_t qt;
    demi_qresult_t qr;
    int ret;

    for (int i = 0; i < iovcnt; i++) {
        data_len += iov[i].iov_len;
    }
    sga = demi_sgaalloc(data_len);

    char *offset = (char *)sga.sga_segs[0].sgaseg_buf;
    for (int i = 0; i < iovcnt; i++) {
        memcpy(offset, iov[i].iov_base, iov[i].iov_len);
        offset += iov[i].iov_len;
    }

#ifdef __DEMIKERNEL_LOG_IO__
    eprintln_cmd(sga.sga_segs[0].sgaseg_buf, offset - (char *)sga.sga_segs[0].sgaseg_buf, 1);
#endif /* __DEMIKERNEL_LOG_IO__ */

    redis_log("HIREDIS demiSocketWritev\n");

    if (((ret = demi_push(&qt, conn->fd, &sga)) != 0 ||
         (ret = demi_wait(&qr, qt)) != 0) &&
        errno != EAGAIN) {
        conn->last_errno = ret;
        fprintf(stderr, "demiSocketWritev error QD %d: %s", conn->fd, strerror(ret));
        return -1;
    }

    // Store push qtoken to be waited on later.
    push_qtoken(qt);

    demi_sgafree(&sga);
    return data_len;
}

static int demiSocketRead(connection *conn, void *buf, size_t buf_len) {
    UNUSED(conn);

    /* We're storing the result from the last wait in a global variable */
    demi_qresult_t *qr = recent_qrs_pop();

    if (qr->qr_value.sga.sga_segs[0].sgaseg_len == 0 ||
        qr->qr_value.sga.sga_segs[0].sgaseg_buf == NULL ||
        qr->qr_opcode != DEMI_OPC_POP) {
        //        conn->state = CONN_STATE_CLOSED;
        redis_log("REDIS read(qd %d, qt %lu) invalid\n", qr->qr_qd, qr->qr_qt);
        return 0;
    }
    
    redis_log("REDIS read(qd %d, qt %lu) from %p %u\n", qr->qr_qd, qr->qr_qt, qr->qr_value.sga.sga_segs[0].sgaseg_buf, qr->qr_value.sga.sga_segs[0].sgaseg_len);

    /* we can't do more sophisticated error handling yet
        else if (ret < 0 && errno != EAGAIN) {
            conn->last_errno = errno;

            Don't overwrite the state of a connection that is not already
            connected, not to mess with handler callbacks.
            
            if (errno != EINTR && conn->state == CONN_STATE_CONNECTED)
                conn->state = CONN_STATE_ERROR;
        } */

    /*  Irene: Assume only one scatter gather element */
    size_t read_len = qr->qr_value.sga.sga_segs[0].sgaseg_len;
    if (read_len > buf_len) {
        // panic?
        fprintf(stderr, "[LOG] demiSocketRead(): read_len > buf_len\n");
    } else {
        memcpy(buf, qr->qr_value.sga.sga_segs[0].sgaseg_buf, read_len);
    }

#ifdef __DEMIKERNEL_LOG_IO__
    eprintln_cmd(buf, read_len, 0);
#endif /* __DEMIKERNEL_LOG_IO__ */

    redis_log("REDIS read cmd = ");
    redis_logn_cmd(buf, read_len, 1);

    //Irene: Use memory freely for debugging
    demi_sgafree(&qr->qr_value.sga);
    return read_len;
}

static int demiSocketAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    int ret = C_OK;

    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;

    connIncrRefs(conn);
    if (!callHandler(conn, accept_handler)) ret = C_ERR;
    connDecrRefs(conn);

    return ret;
}

/* We probably don't need a Demikernel specific function but the conn
   function is not available here */ 
static int demiSocketSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
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

static int demiSocketSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    if (func == conn->read_handler) return C_OK;

    conn->read_handler = func;
    if (!conn->read_handler)
        aeDeleteFileEvent(server.el,conn->fd,AE_READABLE);
    else
        if (aeCreateFileEvent(server.el,conn->fd,
                    AE_READABLE,conn->type->ae_handler,conn) == AE_ERR) return C_ERR;
    return C_OK;
}

static const char *demiSocketGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static void demiSocketEventHandler(struct aeEventLoop *el, int fd, void *clientData, int mask) {
    UNUSED(el);
    UNUSED(fd);
    connection *conn = clientData;
    //recent_qr = &el->fired[0].qr;
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

static int demiSocketBlockingConnect(connection *conn, const char *addr, int port, long long timeout) {
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

/* Not sure if we need these. Just panic for now */

static ssize_t demiSocketSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    //    panic("[LOG] demiConn: demiSocketSyncWrite not supported!");
    UNUSED(timeout);

    return demiSocketWrite(conn, ptr, size);
}

static ssize_t demiSocketSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    fprintf(stderr, "HIREDIS demiSocketSyncRead()\n");
    UNUSED(timeout);

    demi_qtoken_t qt;
    demi_qresult_t qr;
    int ret;

    if (((ret = demi_pop(&qt, conn->fd)) != 0 ||
         (ret = demi_wait(&qr, qt)) != 0 ||
         qr.qr_opcode != DEMI_OPC_POP) &&
        ret != EAGAIN) {
        conn->last_errno = ret;
        return -1;
    }

    if (qr.qr_value.sga.sga_segs[0].sgaseg_len == 0 ||
        qr.qr_value.sga.sga_segs[0].sgaseg_buf == NULL) {
        //        conn->state = CONN_STATE_CLOSED;
        return 0;
    }

    ssize_t read_len = qr.qr_value.sga.sga_segs[0].sgaseg_len;
    if (read_len > size) {
        // panic?
        fprintf(stderr, "[LOG] demiSocketRead(): read_len > buf_len\n");
    } else {
        memcpy(ptr, qr.qr_value.sga.sga_segs[0].sgaseg_buf, read_len);
    }

#ifdef __DEMIKERNEL_LOG_IO__
    eprintln_cmd(ptr, read_len, 0);
#endif /* __DEMIKERNEL_LOG_IO__ */

    //demi_sgafree(&qr.qr_value.sga);
    return read_len;
}

static ssize_t demiSocketSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    fprintf(stderr, "demiSocketSyncReadLine()\n");
    ssize_t nread = 0;

    size--;
    while(size) {
        char c;

        if (demiSocketSyncRead(conn, &c, 1, timeout) == -1) return -1;
        if (c == '\n') {
            *ptr = '\0';
            if (nread && *(ptr - 1) == '\r') *(ptr - 1) = '\0';
            return nread;
        } else {
            *ptr++ = c;
            *ptr = '\0';
            nread++;
        }
        size--;
    }
    return nread;
}

static const char *demiSocketGetType(connection *conn) {
    UNUSED(conn);

    return CONN_TYPE_DEMI;
}

static void demiSocketAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    UNUSED(privdata);

    /* BAD HACK: grab the result from globally stored recent results. */
    demi_qresult_t *qr = recent_qrs_pop(); //&el->fired[0].qr;
    int cfd = qr->qr_value.ares.qd;
    struct sockaddr_in *s = &qr->qr_value.ares.addr;
    char cip[NET_IP_STR_LEN];

    /* convert IP to string */
    inet_ntop(AF_INET,(void*)&(s->sin_addr),cip,NET_IP_STR_LEN);
    //fprintf(stderr, "Demikernel Accepted %s:%d cfd=%u\n", cip, ntohs(s->sin_port), cfd);
    //serverLog(LL_VERBOSE,"Accepted %s:%d", cip, ntohs(s.sin_port));
    acceptCommonHandler(connCreateAcceptedDemiQ(cfd, NULL),0,cip);
}

static int demiSocketAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    // printf("[LOG] demiSocketAddr() called\n");
    // TEMP
    memcpy(ip, "127.0.0.1:4000", 15);
    return C_OK;

    if (anetFdToString(conn->fd, ip, ip_len, port, remote) == 0)
        return C_OK;

    conn->last_errno = errno;
    return C_ERR;
}

static int demiSocketListen(connListener *listener) {
    return listenToPort(listener);
}

static int demiSocketIsLocal(connection *conn) {
    char cip[NET_IP_STR_LEN + 1] = { 0 };

    if (demiSocketAddr(conn, cip, sizeof(cip) - 1, NULL, 1) == C_ERR)
        return -1;

    return !strncmp(cip, "127.", 4) || !strcmp(cip, "::1");
}

static void demiSocketShutdown(connection *conn) {
    if (conn->fd == -1) return;

    shutdown(conn->fd, SHUT_RDWR);
}

static ConnectionType CT_DemiSocket = {
    /* connection type */
    .get_type = demiSocketGetType,

    /* connection type initialize & finalize & configure */
    .init = NULL,
    .cleanup = NULL,
    .configure = NULL,

    /* ae & accept & listen & error & address handler */
    .ae_handler = demiSocketEventHandler,
    .accept_handler = demiSocketAcceptHandler,
    .addr = demiSocketAddr,
    .is_local = demiSocketIsLocal,
    .listen = demiSocketListen,

    /* create/shutdown/close connection */
    .conn_create = connCreateDemikernelSocket,
    .conn_create_accepted = connCreateAcceptedDemiQ,
    .shutdown = demiSocketShutdown,
    .close = demiSocketClose,

    /* connect & accept */
    .connect = demiSocketConnect,
    .blocking_connect = demiSocketBlockingConnect,
    .accept = demiSocketAccept,

    /* IO */
    .write = demiSocketWrite,
    .writev = demiSocketWritev,
    .read = demiSocketRead,
    .set_write_handler = demiSocketSetWriteHandler,
    .set_read_handler = demiSocketSetReadHandler,
    .get_last_error = demiSocketGetLastError,
    .sync_write = demiSocketSyncWrite,
    .sync_read = demiSocketSyncRead,
    .sync_readline = demiSocketSyncReadLine,

    /* pending data */
    .has_pending_data = NULL,
    .process_pending_data = NULL,
};

int demiGetSocketError(connection *conn) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);

    // Irene: Do we have get sockopt?
    if (getsockopt(conn->fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    return sockerr;
}

int RedisRegisterConnectionTypeDemi(void) {
    return connTypeRegister(&CT_DemiSocket);
}