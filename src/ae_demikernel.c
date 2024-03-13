/* Demikernel wait based ae.c module
 *
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

#include <demi/wait.h>
#include <demi/libos.h>
#include <string.h>

/* BIG HACK: We'll just keep the result here for now */
demi_qresult_t recent_qrs[MAX_QTOKEN_COUNT];
size_t recent_qrs_count;
size_t recent_qrs_index;

demi_qresult_t *recent_qrs_pop(void) {
    demi_qresult_t *popped;
    while(1) {
        if(recent_qrs_index >= recent_qrs_count) panic("Popped empty recent_qrs");

        redis_log("REDIS recent_qrs_pop index %lu\n", recent_qrs_index);
        popped = recent_qrs + recent_qrs_index;
        recent_qrs_index += 1;

        if(popped->qr_opcode == DEMI_OPC_INVALID) {
            redis_log("REDIS invalid recent_qrs_pop skipped\n");
        } else {
            break;
        }
    }
    return popped;
}

demi_qtoken_t qtokens[MAX_QTOKEN_COUNT];
size_t qtoken_count = 0;

void push_qtoken(demi_qtoken_t qt) {
    if(qtoken_count == MAX_QTOKEN_COUNT) panic("Too many push qtokens");
    qtokens[qtoken_count++] = qt;
}

inline void reset_qtokens(void) {
    qtoken_count = 0;
}


//===============================================================

typedef struct aeApiState {
    /* map of fds to mask */
    int *fd_mask_map;
    /* number of qtokens actively in use */
    size_t num_qtokens;
    /* list of active qtokens */
    demi_qtoken_t *qtokens;
    /* mapping from file descriptor to offset in the qtoken array */
    int *fd_to_qtoken;
    /* mapping from qtoken array to file descriptor*/
    int *qtoken_to_fd;
    /* fd's with a writable event */
    int *writable_fd_list;
    size_t num_writable_fds;    
} aeApiState;

static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    /* allocate queue descriptor tracking */
    state->fd_to_qtoken = zcalloc(sizeof(int) * eventLoop->setsize);
    state->writable_fd_list = zcalloc(sizeof(int) * eventLoop->setsize);
    state->fd_mask_map = zcalloc(sizeof(bool) * eventLoop->setsize);
    if (state->fd_to_qtoken == NULL ||
        state->writable_fd_list == NULL ||
        state->fd_mask_map == NULL) {
        zfree(state->fd_to_qtoken);
        zfree(state->writable_fd_list);
        zfree(state->fd_mask_map);
        zfree(state);
        return -1;
    }
    state->num_qtokens = 0;
    state->qtokens = NULL;
    state->qtoken_to_fd = NULL;
    state->num_writable_fds = 0;
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    /* adjust max queue descriptor tracking, got this from ae_epoll
       zmalloc comment says that the array gets zero'd but I think it
       is not */
    state->fd_to_qtoken = zrealloc(state->fd_to_qtoken, sizeof(int) * setsize);
    state->writable_fd_list = zrealloc(state->writable_fd_list, sizeof(int) * setsize);
    state->fd_mask_map = zrealloc(state->fd_mask_map, sizeof(bool) * setsize);
    return 0;
}

static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    zfree(state->qtokens);
    zfree(state->fd_to_qtoken);
    zfree(state->qtoken_to_fd);
    zfree(state->writable_fd_list);
    zfree(state->fd_mask_map);
    zfree(state);
}

static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    /* we only allow one event of each type so, check if some of the
       bits are already set */
    if ((mask & AE_READABLE) && !(state->fd_mask_map[fd] & AE_READABLE)) {
        demi_qtoken_t qt = 1110; // for debugging
        int ret = 0;
        /* BIG HACK: we always use the first queue descriptor for
           listening, so we know to call accept instead of pop */
        if (fd == 0)
            ret = demi_accept(&qt, fd);
        else ret = demi_pop(&qt, fd);

        if (ret != 0) return -1;

        /* increase the number of qtokens that we are tracking now */
        int i = state->num_qtokens++;

        /* increase the size of the qtoken arrays */
        state->qtokens = zrealloc(state->qtokens, sizeof(demi_qtoken_t) * state->num_qtokens);
        state->qtoken_to_fd = zrealloc(state->qtoken_to_fd, sizeof(int) * state->num_qtokens);
        /* place the qtoken at the end */
        state->qtokens[i] = qt;
        /* update our map */
        state->qtoken_to_fd[i] = fd;
        state->fd_to_qtoken[fd] = i;
    }

    if ((mask & AE_WRITABLE) && !(state->fd_mask_map[fd] & AE_WRITABLE)) {
        int i = state->num_writable_fds;

        state->num_writable_fds += 1;
        state->writable_fd_list[i] = fd;
    }

    state->fd_mask_map[fd] = state->fd_mask_map[fd] | mask;
    //printf("Add (mask=%u): num read events=%lu num write events=%lu\n", mask, state->num_qtokens, state->num_writable_fds);
    return 0;
}

static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    aeApiState *state = eventLoop->apidata;
    if ((delmask & AE_READABLE) && (state->fd_mask_map[fd] & AE_READABLE)) {
        /* grab the offset of the qtoken that we are removing */
        int i = state->fd_to_qtoken[fd];
        /* zero it out since we are no longer using it */
        state->fd_to_qtoken[fd] = 0;

        /* update the size of the qtoken array and grab the index of
           the last qtoken to move it into the empty space*/
        int moving_qtoken = --state->num_qtokens;
        if (i != moving_qtoken) {
            /* move last element to the one that we are removing
               (unless it already is the last element)*/
            state->qtokens[i] = state->qtokens[moving_qtoken];
            /* update our mappings */
            int moving_fd = state->qtoken_to_fd[moving_qtoken];
            state->qtoken_to_fd[i] = moving_fd;
            state->fd_to_qtoken[moving_fd] = i;
        }
        /* resize the qtoken arrays */
        state->qtokens = zrealloc(state->qtokens, sizeof(demi_qtoken_t) * state->num_qtokens);
        state->qtoken_to_fd = zrealloc(state->qtoken_to_fd, sizeof(int) * state->num_qtokens);        
    }

    if ((delmask & AE_WRITABLE) && (state->fd_mask_map[fd] & AE_WRITABLE)) {
        int last_index = --state->num_writable_fds;
        for (int i = 0; i < last_index; i++) {
            if (state->writable_fd_list[i] == fd) {
                state->writable_fd_list[i] = state->writable_fd_list[last_index];
            }
        }
    }
    state->fd_mask_map[fd] = state->fd_mask_map[fd] & (~delmask);
    //printf("Delete(mask=%u): num read events=%lu num write events=%lu\n", delmask, state->num_qtokens, state->num_writable_fds);
}

#include <stdlib.h>

static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    (void) tvp;

    static size_t ready_offsets_storage[MAX_QTOKEN_COUNT];
    size_t *ready_offsets = ready_offsets_storage;

    #ifdef __MANUAL_TCPMIG__
    static int mig_per_n[4096] = {};
    #endif

    aeApiState *state = eventLoop->apidata;
    int retval = 0;
    demi_qresult_t *qrs = recent_qrs;
    recent_qrs_count = MAX_QTOKEN_COUNT;

    // setenv("WHICH_WAIT", "push", 1);

    // // Wait for push tokens.
    // retval = demi_wait_any(qresults, ready_offsets, &recent_qrs_count, qtokens, qtoken_count, 0);
    // if(retval != 0 || recent_qrs_count != qtoken_count) {
    //     panic("Push tokens wait error");
    // }
    // unsetenv("WHICH_WAIT");

    // #ifdef __REDIS_LOG__
    // redis_log("REDIS after push event loop: %lu results\n", recent_qrs_count);
    // #endif

    // size_t fired_index = 0;
    // for(size_t j = 0; j < recent_qrs_count; j++) {
    //     const demi_qresult_t *qr = &qresults[j];
    //     if(qr->qr_opcode == DEMI_OPC_FAILED) {
    //         #ifdef __DEMIKERNEL_TCPMIG__
    //         if(qr->qr_value.err == ETCPMIG) {
    //             eventLoop->fired[fired_index].fd = qr->qr_qd;
    //             eventLoop->fired[fired_index].mask = state->fd_mask_map[qr->qr_qd] | (1 << 10);
    //             fired_index += 1;
    //             redis_log("REDIS ETCPMIG (push) %d\n", qr->qr_qd);
    //         } else {
    //             panic("push qtoken poll failed, %s", strerror(qr->qr_value.err));
    //         }
    //         #else
    //         panic("push qtoken poll failed, %s", strerror(qr->qr_value.err));
    //         #endif
    //     }
    // }
    // reset_qtokens();
    // recent_qrs_count = MAX_QTOKEN_COUNT;

    if (state->num_writable_fds > 0) {
        eventLoop->fired[0].fd = state->writable_fd_list[0];
        eventLoop->fired[0].mask = AE_WRITABLE;
        recent_qrs_index = 0;
        recent_qrs_count = 0;
        return 1;
    } else if (state->num_qtokens > 0 || qtoken_count > 0) {
        redis_log("Push qtokens (%lu): ", qtoken_count);
        for(size_t i = 0; i < qtoken_count; i++) {
            redis_log("%lu ", qtokens[i]);
        }
        redis_log(",  ");

        redis_log("Pop qtokens (%lu): ", state->num_qtokens);
        for(size_t i = 0; i < state->num_qtokens; i++) {
            redis_log("%lu ", state->qtokens[i]);
        }
        redis_log("\n");

        // Copy qtokens from event loop state to global list of qtokens.
        memcpy(&qtokens[qtoken_count], state->qtokens, state->num_qtokens * sizeof(demi_qtoken_t));
        size_t push_qtoken_count = qtoken_count;
        qtoken_count += state->num_qtokens;

        memset(qrs, 0, sizeof(demi_qresult_t) * MAX_QTOKEN_COUNT);

        setenv("WHICH_WAIT", "normal", 1);
        retval = demi_wait_any(qrs, ready_offsets, &recent_qrs_count, qtokens, qtoken_count, 500);
        unsetenv("WHICH_WAIT");
        recent_qrs_index = 0;

        if (retval == 0) {
            // Ensure that all push operations have been completed and removed.
            if(recent_qrs_count < push_qtoken_count) {
                panic("all push qtokens not completed");
            }
            redis_log("DONE Push qtokens: ");
            for(size_t j = 0; j < push_qtoken_count; j++) {
                if(ready_offsets[j] != j) {
                    panic("all push qtokens not completed");
                }
                // Remove each push qresult.
                demi_qresult_t *r = recent_qrs_pop();
                redis_log("%lu(%lu) ", r->qr_qt, qtokens[j]);
            }
            redis_log("\n");

            // Now we can safely ignore the push qtokens and focus on the pop/accept qtokens.
            reset_qtokens();
            size_t qrs_count = recent_qrs_count - recent_qrs_index;
            ready_offsets += push_qtoken_count;
            qrs += push_qtoken_count;

            redis_log("REDIS poll results: %lu = %lu (push) + %lu (pop)\n", recent_qrs_count, push_qtoken_count, qrs_count);

            redis_log("DONE Pop qtokens: ");
            for(size_t i = 0; i < qrs_count; i++) {
                redis_log("[%d]%lu(%lu) ", qrs[i].qr_qd, qrs[i].qr_qt, qtokens[ready_offsets[i]]);
            }
            redis_log("\n");

            // #ifdef __REDIS_LOG__
            // redis_log("REDIS after pop event loop: %lu results\n", recent_qrs_count);
            // #endif

            // Get rid of all push qtoken results first.
            // size_t fired_index = 0;
            // for(size_t j = 0; j < push_qtoken_count; j++) {
            //     const demi_qresult_t *qr = &qresults[j];
            //     if(qr->qr_opcode == DEMI_OPC_FAILED) {
            //         #ifdef __DEMIKERNEL_TCPMIG__
            //         if(qr->qr_value.err == ETCPMIG) {
            //             eventLoop->fired[fired_index].fd = qr->qr_qd;
            //             eventLoop->fired[fired_index].mask = state->fd_mask_map[qr->qr_qd] | (1 << 10);
            //             fired_index += 1;
            //             redis_log("REDIS ETCPMIG (push) %d\n", qr->qr_qd);
            //         } else {
            //             panic("push qtoken poll failed, %s", strerror(qr->qr_value.err));
            //         }
            //         #else
            //         panic("push qtoken poll failed, %s", strerror(qr->qr_value.err));
            //         #endif
            //     }
            // }

            for(size_t j = 0; j < qrs_count; j++) {
                demi_qresult_t *qr = &qrs[j];

                // We subtract number of push tokens because ready offsets are from the start of the full array, including push tokens.
                int ready_offset = ready_offsets[j] - push_qtoken_count;

                int mask = state->fd_mask_map[qr->qr_qd];
                demi_qtoken_t qt = 100; // for debugging
                if (qr->qr_opcode == DEMI_OPC_POP) {
                    /* if no buffer is returned, then there was an error */            
                    if (qr->qr_value.sga.sga_segs[0].sgaseg_len == 0 ||
                        qr->qr_value.sga.sga_segs[0].sgaseg_buf == NULL ||
                        qr->qr_opcode == 5) {
                        state->qtokens[ready_offset] = 0;
                    } else {
                        #ifdef __MANUAL_TCPMIG__
                        if(mig_per_n[qr->qr_qd] == 1) {
                            demi_initiate_migration(qr->qr_qd);
                            mig_per_n[qr->qr_qd] = 0;
                            mask |= (1 << 10);
                        }
                        retval = 0;
                        #else
                        redis_log("REDIS pop %d\n", qr->qr_qd);
                        retval = demi_pop(&qt, qr->qr_qd);
                        state->qtokens[ready_offset] = qt;
                        #endif
                    }
                } else if (qr->qr_opcode == DEMI_OPC_ACCEPT) {
                    retval = demi_accept(&qt, qr->qr_qd);
                    state->qtokens[ready_offset] = qt;
                    redis_log("REDIS accept %d\n", qr->qr_value.ares.qd);

                    #ifdef __MANUAL_TCPMIG__
                    mig_per_n[qr->qr_value.ares.qd] = 1;
                    #endif
                } else if (qr->qr_opcode == DEMI_OPC_FAILED) {
                    #ifdef __DEMIKERNEL_TCPMIG__
                    if (qr->qr_value.err == ETCPMIG) {
                        // We don't track this operation anymore.
                        mask |= (1 << 10);
                        // Set opcode to invalid so recent_qrs_pop() skips this.
                        qr->qr_opcode = DEMI_OPC_INVALID;
                        redis_log("REDIS ETCPMIG %d\n", qr->qr_qd);
                    }
                    else {
                        panic("aeApiPoll: poll failed pop/accept, %s", strerror(qr->qr_value.err));
                    }
                    #else
                    panic("aeApiPoll: poll failed pop/accept, %s", strerror(qr->qr_value.err));
                    #endif
                } else {
                    panic("aeApiPoll: invalid qresult opcode: %d", qr->qr_opcode);
                }
                if (retval != 0) {
                    /* Not sure if this is the right way to indicate an error */            
                    panic("aeApiPoll: pop/accept, %s", strerror(retval));
                }
                eventLoop->fired[j].fd = qr->qr_qd;
                eventLoop->fired[j].mask = mask;
            }
        } else {
            panic("aeApiPoll: demi_wait_any, %s", strerror(retval));
        }
    } else {
	    panic("aeApiPoll: no events!");
    }

    return recent_qrs_count - recent_qrs_index;
}

static char *aeApiName(void) {
    return "demi_wait_any";
}