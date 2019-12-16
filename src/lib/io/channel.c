/*
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program; if not, write to the Free Software
 *   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */

/**
 * $Id$
 *
 * @brief Two-way thread-safe channels.
 * @file io/channel.c
 *
 * @copyright 2016 Alan DeKok (aland@freeradius.org)
 */
RCSID("$Id$")

#include <freeradius-devel/io/channel.h>
#include <freeradius-devel/io/control.h>
#include <freeradius-devel/util/log.h>
#include <freeradius-devel/server/rad_assert.h>

#ifdef HAVE_STDATOMIC_H
#  include <stdatomic.h>
#else
#  include <freeradius-devel/util/stdatomic.h>
#endif

/*
 *	Debugging, mainly for channel_test
 */
#ifdef DEBUG_CHANNEL
#define MPRINT(...) fprintf(stdout, __VA_ARGS__)
#else
#define MPRINT(...)
#endif

/*
 *	We disable this until we fix all of the signaling issues...
 */
#define ENABLE_SKIPS (0)

typedef enum {
	TO_RESPONDER = 0,
	TO_REQUESTOR = 1
} fr_channel_direction_t;

#ifdef DEBUG_CHANNEL
static fr_table_num_sorted_t const channel_direction[] = {
	{ "to responder",	TO_RESPONDER },
	{ "to requestor",	TO_REQUESTOR },
};
size_t channel_direction_len = NUM_ELEMENTS(channel_direction);
#endif

#if 0
#define SIGNAL_INTERVAL (1000000)	//!< The minimum interval between responder signals.
#endif

/** Size of the atomic queues
 *
 * The queue reader MUST service the queue occasionally,
 * otherwise the writer will not be able to write.  If it's too
 * low, the writer will fail.  If it's too high, it will
 * unnecessarily use memory.  So we're better off putting it on
 * the high side.
 *
 * The reader SHOULD service the queues at inter-packet latency.
 * i.e. at 1M pps, the queue will get serviced every microsecond.
 */
#define ATOMIC_QUEUE_SIZE (1024)

typedef enum fr_channel_signal_t {
	FR_CHANNEL_SIGNAL_ERROR			= FR_CHANNEL_ERROR,
	FR_CHANNEL_SIGNAL_DATA_TO_RESPONDER	= FR_CHANNEL_DATA_READY_RESPONDER,
	FR_CHANNEL_SIGNAL_DATA_TO_REQUESTOR	= FR_CHANNEL_DATA_READY_REQUESTOR,
	FR_CHANNEL_SIGNAL_OPEN			= FR_CHANNEL_OPEN,
	FR_CHANNEL_SIGNAL_CLOSE			= FR_CHANNEL_CLOSE,

	/*
	 *	The preceding MUST be in the same order as fr_channel_event_t
	 */

	FR_CHANNEL_SIGNAL_DATA_DONE_RESPONDER,
	FR_CHANNEL_SIGNAL_RESPONDER_SLEEPING,
} fr_channel_signal_t;

typedef struct {
	fr_channel_signal_t	signal;		//!< the signal to send
	uint64_t		ack;		//!< or the endpoint..
	fr_channel_t		*ch;		//!< the channel
} fr_channel_control_t;

/** One end of a channel
 *
 * Consists of a kqueue descriptor, and an atomic queue.
 * The atomic queue is there to get bulk data through, because it's more efficient
 * than pushing 1M+ events per second through a kqueue.
 */
typedef struct {
	fr_channel_direction_t	direction;	//!< Use for debug messages.

	fr_control_t		*control;	//!< The control plane, consisting of an atomic queue and kqueue.

	fr_ring_buffer_t	*rb;		//!< Ring buffer for control-plane messages.

	void			*uctx;		//!< Worker context.

	fr_channel_recv_callback_t recv;	//!< callback for receiving messages
	void			*recv_uctx;	//!< context for receiving messages

	int			num_outstanding; //!< Number of outstanding requests with no reply.
	bool			must_signal;	//!< we need to signal the other end

	uint64_t		num_signals;	//!< Number of kevent signals we've sent.

	uint64_t		num_resignals;	//!< Number of signals resent.

	uint64_t		num_kevents;	//!< Number of times we've looked at kevents.

	uint64_t		sequence;	//!< Sequence number for this channel.
	uint64_t		ack;		//!< Sequence number of the other end.
	atomic_uint_fast64_t	their_view_of_my_sequence;	//!< Should be clear.

	uint64_t		sequence_at_last_signal;	//!< When we last signaled.

	uint64_t		num_packets;	//!< Number of actual data packets.

	fr_time_t		last_write;	//!< Last write to the channel.
	fr_time_t		last_read_other; //!< Last time we successfully read a message from the other the channel
	fr_time_delta_t		message_interval; //!< Interval between messages.

	fr_time_t		last_sent_signal; //!< The last time when we signaled the other end.

	fr_atomic_queue_t	*aq;		//!< The queue of messages - visible only to this channel.

} fr_channel_end_t;

typedef struct fr_channel_s fr_channel_t;

/** A full channel, which consists of two ends
 *
 * A channel consists of an I/O identifier that can be placed in kequeue
 * and an atomic queue in each direction to allow for bidirectional communication.
 */
struct fr_channel_s {
	fr_time_t		cpu_time;	//!< Total time used by the responder for this channel.
	fr_time_t		processing_time; //!< Time spent by the responder processing requests.

	atomic_bool		active;		//!< Whether the channel is active.
	bool			same_thread;	//!< are both ends in the same thread?

	fr_channel_end_t	end[2];		//!< Two ends of the channel.
};

#ifdef DEBUG_CHANNEL
static fr_table_num_sorted_t const channel_signals[] = {
	{ "error",			FR_CHANNEL_ERROR			},
	{ "data-to-responder",		FR_CHANNEL_SIGNAL_DATA_TO_RESPONDER	},
	{ "data-to-requestor",		FR_CHANNEL_DATA_READY_REQUESTOR		},
	{ "open",			FR_CHANNEL_OPEN				},
	{ "close",			FR_CHANNEL_CLOSE			},
	{ "data-done-responder",	FR_CHANNEL_SIGNAL_DATA_DONE_RESPONDER	},
	{ "responder-sleeping",		FR_CHANNEL_SIGNAL_RESPONDER_SLEEPING	},
};
size_t const channel_signals_len = NUM_ELEMENTS(channel_signals);
#endif

fr_table_num_sorted_t const channel_packet_priority[] = {
	{ "high",	PRIORITY_HIGH		},
	{ "low",	PRIORITY_LOW		},
	{ "normal",	PRIORITY_NORMAL		},
	{ "now",	PRIORITY_NOW		}
};
size_t channel_packet_priority_len = NUM_ELEMENTS(channel_packet_priority);


/** Create a new channel
 *
 * @param[in] ctx	The talloc_ctx to allocate channel data in.
 * @param[in] requestor	control plane.
 * @param[in] responder	control plane.
 * @param[in] same	whether or not the channel is for the same thread
 * @return
 *	- NULL on error
 *	- channel on success
 */
fr_channel_t *fr_channel_create(TALLOC_CTX *ctx, fr_control_t *requestor, fr_control_t *responder, bool same)
{
	fr_time_t now;
	fr_channel_t *ch;

	ch = talloc_zero(ctx, fr_channel_t);
	if (!ch) {
	nomem:
		fr_strerror_printf("Failed allocating memory");
		return NULL;
	}

	ch->same_thread = same;

	ch->end[TO_RESPONDER].direction = TO_RESPONDER;
	ch->end[TO_REQUESTOR].direction = TO_REQUESTOR;

	ch->end[TO_RESPONDER].aq = fr_atomic_queue_create(ch, ATOMIC_QUEUE_SIZE);
	if (!ch->end[TO_RESPONDER].aq) {
		talloc_free(ch);
		goto nomem;
	}

	ch->end[TO_REQUESTOR].aq = fr_atomic_queue_create(ch, ATOMIC_QUEUE_SIZE);
	if (!ch->end[TO_REQUESTOR].aq) {
		talloc_free(ch);
		goto nomem;
	}

	ch->end[TO_RESPONDER].control = responder;
	ch->end[TO_REQUESTOR].control = requestor;

	/*
	 *	Create the ring buffer for the requestor to send
	 *	control-plane messages to the responder, and vice-versa.
	 */
	ch->end[TO_RESPONDER].rb = fr_ring_buffer_create(ch, FR_CONTROL_MAX_MESSAGES * FR_CONTROL_MAX_SIZE);
	if (!ch->end[TO_RESPONDER].rb) {
	rb_nomem:
		fr_strerror_printf_push("Failed allocating ring buffer");
		talloc_free(ch);
		return NULL;
	}

	ch->end[TO_REQUESTOR].rb = fr_ring_buffer_create(ch, FR_CONTROL_MAX_MESSAGES * FR_CONTROL_MAX_SIZE);
	if (!ch->end[TO_REQUESTOR].rb) {
		talloc_free(ch);
		goto rb_nomem;
	}

	/*
	 *	Initialize all of the timers to now.
	 */
	now = fr_time();

	ch->end[TO_RESPONDER].last_write = now;
	ch->end[TO_RESPONDER].last_read_other = now;
	ch->end[TO_RESPONDER].last_sent_signal = now;

	ch->end[TO_REQUESTOR].last_write = now;
	ch->end[TO_REQUESTOR].last_read_other = now;
	ch->end[TO_REQUESTOR].last_sent_signal = now;

	atomic_store(&ch->active, true);

	return ch;
}


/** Send a message via a kq user signal
 *
 * Note that the caller doesn't care about data in the event, that is
 * sent via the atomic queue.  The kevent code takes care of
 * delivering the signal once, even if it's sent by multiple requestor
 * threads.
 *
 * The thread watching the KQ knows which end it is.  So when it gets
 * the signal (and the channel pointer) it knows to look at end[0] or
 * end[1].  We also send which end in 'which' (0, 1) to further help
 * the recipient.
 *
 * @param[in] ch	the channel.
 * @param[in] when	the data was ready.  Typically taken from the message.
 * @param[in] end	of the channel that the message was written to.
 * @param[in] which	end of the channel (0/1).
 * @return
 *	- <0 on error
 *	- 0 on success
 */
static int fr_channel_data_ready(fr_channel_t *ch, fr_time_t when, fr_channel_end_t *end, fr_channel_signal_t which)
{
	fr_channel_control_t cc;

	end->last_sent_signal = when;
	end->num_signals++;
	end->must_signal = false;

	cc.signal = which;
	cc.ack = end->ack;
	cc.ch = ch;

	MPRINT("Signalling %s, with %s\n",
	       fr_table_str_by_value(channel_direction, end->direction, "<INVALID>"),
	       fr_table_str_by_value(channel_signals, which, "<INVALID>"));

	return fr_control_message_send(end->control, end->rb, FR_CONTROL_ID_CHANNEL, &cc, sizeof(cc));
}

#define IALPHA (8)
#define RTT(_old, _new) ((_new + ((IALPHA - 1) * _old)) / IALPHA)

/** Send a request message into the channel
 *
 * The message should be initialized, other than "sequence" and "ack".
 *
 * This function automatically calls the recv_reply callback if there is a reply.
 *
 * @param[in] ch	the channel to send the request on.
 * @param[in] cd	the message to send.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_send_request(fr_channel_t *ch, fr_channel_data_t *cd)
{
	uint64_t sequence;
	fr_time_t when, message_interval;
	fr_channel_end_t *requestor;

	/*
	 *	Same thread?  Just call the "recv" function directly.
	 */
	if (ch->same_thread) {
		ch->end[TO_REQUESTOR].recv(ch->end[TO_REQUESTOR].recv_uctx, ch, cd);
		return 0;
	}

	requestor = &(ch->end[TO_RESPONDER]);
	when = cd->m.when;

	sequence = requestor->sequence + 1;
	cd->live.sequence = sequence;
	cd->live.ack = requestor->ack;

	/*
	 *	Push the message onto the queue for the other end.  If
	 *	the push fails, the caller should try another queue.
	 */
	if (!fr_atomic_queue_push(requestor->aq, cd)) {
		fr_strerror_printf("Failed pushing to atomic queue");
		while (fr_channel_recv_reply(ch));
		return -1;
	}

	requestor->sequence = sequence;
	message_interval = when - requestor->last_write;

	if (!requestor->message_interval) {
		requestor->message_interval = message_interval;
	} else {
		requestor->message_interval = RTT(requestor->message_interval, message_interval);
	}

	rad_assert(requestor->last_write <= when);
	requestor->last_write = when;

	requestor->num_outstanding++;
	requestor->num_packets++;

	MPRINT("REQUESTOR requests %"PRIu64", num_outstanding %"PRIu64"\n", requestor->num_packets, requestor->num_outstanding);

#if ENABLE_SKIPS
	/*
	 *	We just sent the first packet.  There can't possibly be a reply, so don't bother looking.
	 */
	if (requestor->num_outstanding == 1) {

		/*
		 *	There is at least one old packet which is
		 *	outstanding, look for a reply.
		 */
	} else if (requestor->num_outstanding > 1) {
		while (fr_channel_recv_reply(ch));

		/*
		 *	There's no reply yet, so we still have packets outstanding.
		 *	Or, there is a reply, and there are more packets outstanding.
		 *	Skip the signal.
		 */
		if (!requestor->must_signal && (!*p_reply || (*p_reply && (requestor->num_outstanding > 1)))) {
			MPRINT("REQUESTOR SKIPS signal\n");
			return 0;
		}
	}
#endif

	/*
	 *	Tell the other end that there is new data ready.
	 *
	 *	Ignore errors on signalling.  The responder already has
	 *	the packet in its inbound queue, so at some point, it
	 *	will pick up the message.
	 */
	MPRINT("REQUESTOR SIGNALS\n");
	(void) fr_channel_data_ready(ch, when, requestor, FR_CHANNEL_SIGNAL_DATA_TO_RESPONDER);
	return 0;
}

/** Receive a reply message from the channel
 *
 * @param[in] ch	the channel to read data from.
 * @return
 *	- true if there was a message received
 *	- false if there are no more messages
 */
bool fr_channel_recv_reply(fr_channel_t *ch)
{
	fr_channel_data_t *cd;
	fr_channel_end_t *requestor;
	fr_atomic_queue_t *aq;

	rad_assert(ch->end[TO_RESPONDER].recv != NULL);

	aq = ch->end[TO_REQUESTOR].aq;
	requestor = &(ch->end[TO_RESPONDER]);

	/*
	 *	It's OK for the queue to be empty.
	 */
	if (!fr_atomic_queue_pop(aq, (void **) &cd)) return false;

	/*
	 *	We want an exponential moving average for round trip
	 *	time, where "alpha" is a number between [0,1)
	 *
	 *	RTT_new = alpha * RTT_old + (1 - alpha) * RTT_sample
	 *
	 *	BUT we use fixed-point arithmetic, so we need to use inverse alpha,
	 *	which works out to the following equation:
	 *
	 *	RTT_new = (RTT_sample + (ialpha - 1) * RTT_old) / ialpha
	 *
	 *	NAKs have zero processing time, so we ignore them for
	 *	the purpose of RTT.
	 */
	if (cd->reply.processing_time) {
		ch->processing_time = RTT(ch->processing_time, cd->reply.processing_time);
	}
	ch->cpu_time = cd->reply.cpu_time;

	/*
	 *	Update the outbound channel with the knowledge that
	 *	we've received one more reply, and with the responders
	 *	ACK.
	 */
	rad_assert(requestor->num_outstanding > 0);
	rad_assert(cd->live.sequence > requestor->ack);
	rad_assert(cd->live.sequence <= requestor->sequence); /* must have fewer replies than requests */

	requestor->num_outstanding--;
	requestor->ack = cd->live.sequence;
	atomic_store(&requestor->their_view_of_my_sequence, cd->live.ack);

	rad_assert(requestor->last_read_other <= cd->m.when);
	requestor->last_read_other = cd->m.when;

	ch->end[TO_RESPONDER].recv(ch->end[TO_RESPONDER].recv_uctx, ch, cd);

	return true;
}


/** Receive a request message from the channel
 *
 * @param[in] ch the channel
 * @return
 *	- true if there was a message received
 *	- false if there are no more messages
 */
bool fr_channel_recv_request(fr_channel_t *ch)
{
	fr_channel_data_t *cd;
	fr_channel_end_t *responder;
	fr_atomic_queue_t *aq;

	aq = ch->end[TO_RESPONDER].aq;
	responder = &(ch->end[TO_REQUESTOR]);

	/*
	 *	It's OK for the queue to be empty.
	 */
	if (!fr_atomic_queue_pop(aq, (void **) &cd)) return false;

	rad_assert(cd->live.sequence > responder->ack);
	rad_assert(cd->live.sequence >= responder->sequence); /* must have more requests than replies */

	responder->num_outstanding++;
	responder->ack = cd->live.sequence;
	atomic_store(&responder->their_view_of_my_sequence, cd->live.ack);

	rad_assert(responder->last_read_other <= cd->m.when);
	responder->last_read_other = cd->m.when;

	ch->end[TO_REQUESTOR].recv(ch->end[TO_REQUESTOR].recv_uctx, ch, cd);

	return true;
}

/** Send a reply message into the channel
 *
 * The message should be initialized, other than "sequence" and "ack".
 *
 * @param[in] ch		the channel to send the reply on.
 * @param[in] cd		the message to send
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_send_reply(fr_channel_t *ch, fr_channel_data_t *cd)
{
	uint64_t		sequence;
	fr_time_t		when, message_interval;
	fr_channel_end_t	*responder;
	uint64_t		their_view_of_my_sequence;

	if (!ch->active) {
		fr_strerror_printf("Channel not yet active");
		return -1;
	}

	/*
	 *	Same thread?  Just call the "recv" function directly.
	 */
	if (ch->same_thread) {
		ch->end[TO_RESPONDER].recv(ch->end[TO_RESPONDER].recv_uctx, ch, cd);
		return 0;
	}

	responder = &(ch->end[TO_REQUESTOR]);

	when = cd->m.when;

	sequence = responder->sequence + 1;
	cd->live.sequence = sequence;
	cd->live.ack = responder->ack;

	if (!fr_atomic_queue_push(responder->aq, cd)) {
		fr_strerror_printf("Failed pushing to atomic queue");
		while (fr_channel_recv_request(ch));
		return -1;
	}

	rad_assert(responder->num_outstanding > 0);
	responder->num_outstanding--;
	responder->num_packets++;

	MPRINT("\tRESPONDER replies %"PRIu64", num_outstanding %"PRIu64"\n", responder->num_packets, responder->num_outstanding);

	responder->sequence = sequence;
	message_interval = when - responder->last_write;
	responder->message_interval = RTT(responder->message_interval, message_interval);

	rad_assert(responder->last_write <= when);
	responder->last_write = when;

	/*
	 *	Even if we think we have no more packets to process,
	 *	the caller may have sent us one.  Go check the input
	 *	channel.
	 */
	while (fr_channel_recv_request(ch));

	/*
	 *	No packets outstanding, we HAVE to signal the requestor
	 *	thread.
	 */
	if (responder->num_outstanding == 0) {
		(void) fr_channel_data_ready(ch, when, responder, FR_CHANNEL_SIGNAL_DATA_DONE_RESPONDER);
		return 0;
	}

	MPRINT("\twhen - last_read_other = %"PRIu64" - %"PRIu64" = %"PRIu64"\n", when, responder->last_read_other, when - responder->last_read_other);
	MPRINT("\twhen - last signal = %"PRIu64" - %"PRIu64" = %"PRIu64"\n", when, responder->last_sent_signal, when - responder->last_sent_signal);
	MPRINT("\tsequence - ack = %"PRIu64" - %"PRIu64" = %"PRIu64"\n", responder->sequence, responder->their_view_of_my_sequence, responder->sequence - responder->their_view_of_my_sequence);

	their_view_of_my_sequence = atomic_load(&responder->their_view_of_my_sequence);

	if (responder->sequence_at_last_signal > their_view_of_my_sequence) return 0;

	/*
	 *	If we've received a new packet in the last while, OR
	 *	we've sent a signal in the last while, then we don't
	 *	need to send a new signal.  But we DO send a signal if
	 *	we haven't seen an ACK for a few packets.
	 *
	 *	FIXME: make these limits configurable, or include
	 *	predictions about packet processing time?
	 */
	rad_assert(responder->their_view_of_my_sequence <= responder->sequence);
#if 0
	if (((responder->sequence - their_view_of_my_sequence) <= 1000) &&
	    ((when - responder->last_read_other < SIGNAL_INTERVAL) ||
	     ((when - responder->last_sent_signal) < SIGNAL_INTERVAL))) {
		MPRINT("\tRESPONDER SKIPS signal\n");
		return 0;
	}
#endif

	MPRINT("\tRESPONDER SIGNALS num_outstanding %"PRIu64"\n", responder->num_outstanding);
	(void) fr_channel_data_ready(ch, when, responder, FR_CHANNEL_SIGNAL_DATA_TO_REQUESTOR);
	return 0;
}


/** Don't send a reply message into the channel
 *
 * The message should be the one we received from the network.
 *
 * @param[in] ch		the channel on which we're dropping a packet
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_null_reply(fr_channel_t *ch)
{
	fr_channel_end_t *responder;

	responder = &(ch->end[TO_REQUESTOR]);

	responder->sequence++;
	return 0;
}



/** Signal a channel that the responder is sleeping
 *
 * This function should be called from the responders idle loop.
 * i.e. only when it has nothing else to do.
 *
 * @param[in] ch	the channel to signal we're no longer listening on.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_responder_sleeping(fr_channel_t *ch)
{
	fr_channel_end_t *responder;
	fr_channel_control_t cc;

	responder = &(ch->end[TO_REQUESTOR]);

	/*
	 *	We don't have any outstanding requests to process for
	 *	this channel, don't signal the network thread that
	 *	we're sleeping.  It already knows.
	 */
	if (responder->num_outstanding == 0) return 0;

	responder->num_signals++;

	cc.signal = FR_CHANNEL_SIGNAL_RESPONDER_SLEEPING;
	cc.ack = responder->ack;
	cc.ch = ch;

	MPRINT("\tRESPONDER SLEEPING num_outstanding %"PRIu64", packets in %"PRIu64", packets out %"PRIu64"\n", responder->num_outstanding,
	       ch->end[TO_RESPONDER].num_packets, responder->num_packets);
	return fr_control_message_send(responder->control, responder->rb, FR_CONTROL_ID_CHANNEL, &cc, sizeof(cc));
}


/** Service a control-plane message
 *
 * @param[in] when		The current time.
 * @param[out] p_channel	The channel which should be serviced.
 * @param[in] data		The control message.
 * @param[in] data_size		The size of the control message.
 * @return
 *	- FR_CHANNEL_ERROR on error
 *	- FR_CHANNEL_NOOP, on do nothing
 *	- FR_CHANNEL_DATA_READY on data ready
 *	- FR_CHANNEL_OPEN when a channel has been opened and sent to us
 *	- FR_CHANNEL_CLOSE when a channel should be closed
 */
fr_channel_event_t fr_channel_service_message(fr_time_t when, fr_channel_t **p_channel, void const *data, size_t data_size)
{
	int rcode;
#if ENABLE_SKIPS
	uint64_t ack;
#endif
	fr_channel_control_t cc;
	fr_channel_signal_t cs;
	fr_channel_event_t ce = FR_CHANNEL_ERROR;
	fr_channel_end_t *requestor;
	fr_channel_t *ch;

	rad_assert(data_size == sizeof(cc));
	memcpy(&cc, data, data_size);

	cs = cc.signal;
#if ENABLE_SKIPS
	ack = cc.ack;
#endif
	*p_channel = ch = cc.ch;

	switch (cs) {
	/*
	 *	These all have the same numbers as the channel
	 *	events, and have no extra processing.  We just
	 *	return them as-is.
	 */
	case FR_CHANNEL_SIGNAL_ERROR:
	case FR_CHANNEL_SIGNAL_DATA_TO_RESPONDER:
	case FR_CHANNEL_SIGNAL_DATA_TO_REQUESTOR:
	case FR_CHANNEL_SIGNAL_OPEN:
	case FR_CHANNEL_SIGNAL_CLOSE:
		MPRINT("channel got %d\n", cs);
		return (fr_channel_event_t) cs;

	/*
	 *	Only sent by the responder.  Both of these
	 *	situations are largely the same, except for
	 *	return codes.
	 */
	case FR_CHANNEL_SIGNAL_DATA_DONE_RESPONDER:
		MPRINT("channel got data_done_responder\n");
		ce = FR_CHANNEL_DATA_READY_REQUESTOR;
		ch->end[TO_RESPONDER].must_signal = true;
		break;

	case FR_CHANNEL_SIGNAL_RESPONDER_SLEEPING:
		MPRINT("channel got responder_sleeping\n");
		ce = FR_CHANNEL_NOOP;
		ch->end[TO_RESPONDER].must_signal = true;
		break;
	}

	/*
	 *	Compare their ACK to the last sequence we
	 *	sent.  If it's different, we signal the responder
	 *	to wake up.
	 */
	requestor = &ch->end[TO_RESPONDER];
#if ENABLE_SKIPS
	if (!requestor->must_signal && (ack == requestor->sequence)) {
		MPRINT("REQUESTOR SKIPS signal AFTER CE %d num_outstanding %"PRIu64"\n", cs, requestor->num_outstanding);
		MPRINT("REQUESTOR has ack %"PRIu64", my seq %"PRIu64" my_view %"PRIu64"\n", ack, requestor->sequence, requestor->their_view_of_my_sequence);
		return ce;
	}

	/*
	 *	The responder is sleeping or done.  There are more
	 *	packets available, so we signal it to wake up again.
	 */
	rad_assert(ack <= requestor->sequence);
#endif

	/*
	 *	We're signaling it again...
	 */
	requestor->num_resignals++;

	/*
	 *	The responder hasn't seen our last few packets.  Signal
	 *	that there is data ready.
	 */
	MPRINT("REQUESTOR SIGNALS AFTER CE %d\n", cs);
	rcode = fr_channel_data_ready(ch, when, requestor, FR_CHANNEL_SIGNAL_DATA_TO_RESPONDER);
	if (rcode < 0) return FR_CHANNEL_ERROR;

	return ce;
}


/** Service a control-plane event.
 *
 * The channels use control planes for internal signaling.  Note that
 * the caller does NOT pass the channel into this function.  Instead,
 * the channel is taken from the kevent.
 *
 * @param[in] ch	The channel to service.
 * @param[in] c		The control plane on which we received the kev.
 * @param[in] kev	The kevent data, should get passed to the control plane.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_service_kevent(fr_channel_t *ch, fr_control_t *c, UNUSED struct kevent const *kev)
{
	(void) talloc_get_type_abort(ch, fr_channel_t);

	if (c == ch->end[TO_RESPONDER].control) {
		ch->end[TO_RESPONDER].num_kevents++;
	} else {
		ch->end[TO_REQUESTOR].num_kevents++;
	}

	return 0;
}


/** Check if a channel is active.
 *
 * A channel may be closed by either end.  If so, it stays alive (but
 * inactive) until both ends acknowledge the close.
 *
 * @param[in] ch the channel
 * @return
 *	- false the channel is closing.
 *	- true the channel is active
 */
bool fr_channel_active(fr_channel_t *ch)
{
	return atomic_load(&ch->active);
}

/** Signal a responder that the channel is closing
 *
 * @param[in] ch	The channel.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_signal_responder_close(fr_channel_t *ch)
{
	fr_channel_control_t cc;

	(void) talloc_get_type_abort(ch, fr_channel_t);

	atomic_store(&ch->active, false);

	cc.signal = FR_CHANNEL_SIGNAL_CLOSE;
	cc.ack = TO_RESPONDER;
	cc.ch = ch;

	return fr_control_message_send(ch->end[TO_RESPONDER].control, ch->end[TO_RESPONDER].rb, FR_CONTROL_ID_CHANNEL, &cc, sizeof(cc));
}

/** Acknowledge that the channel is closing
 *
 * @param[in] ch	The channel.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_responder_ack_close(fr_channel_t *ch)
{
	fr_channel_control_t cc;

	(void) talloc_get_type_abort(ch, fr_channel_t);

	atomic_store(&ch->active, false);

	cc.signal = FR_CHANNEL_SIGNAL_CLOSE;
	cc.ack = TO_REQUESTOR;
	cc.ch = ch;

	return fr_control_message_send(ch->end[TO_REQUESTOR].control, ch->end[TO_REQUESTOR].rb, FR_CONTROL_ID_CHANNEL, &cc, sizeof(cc));
}

/** Add responder-specific data to a channel
 *
 * @param[in] ch	The channel.
 * @param[in] uctx	The context to add.
 */
void fr_channel_responder_uctx_add(fr_channel_t *ch, void *uctx)
{
	(void) talloc_get_type_abort(ch, fr_channel_t);

	ch->end[TO_REQUESTOR].uctx = uctx;
}


/** Get responder-specific data from a channel
 *
 * @param[in] ch	The channel.
 */
void *fr_channel_responder_uctx_get(fr_channel_t *ch)
{
	(void) talloc_get_type_abort(ch, fr_channel_t);

	return ch->end[TO_REQUESTOR].uctx;
}


/** Add network-specific data to a channel
 *
 * @param[in] ch	The channel.
 * @param[in] uctx	The context to add.
 */
void fr_channel_requestor_uctx_add(fr_channel_t *ch, void *uctx)
{
	(void) talloc_get_type_abort(ch, fr_channel_t);

	ch->end[TO_RESPONDER].uctx = uctx;
}


/** Get network-specific data from a channel
 *
 * @param[in] ch	The channel.
 */
void *fr_channel_requestor_uctx_get(fr_channel_t *ch)
{
	(void) talloc_get_type_abort(ch, fr_channel_t);

	return ch->end[TO_RESPONDER].uctx;
}


int fr_channel_set_recv_reply(fr_channel_t *ch, void *uctx, fr_channel_recv_callback_t recv_reply)
{
	ch->end[TO_RESPONDER].recv = recv_reply;
	ch->end[TO_RESPONDER].recv_uctx = uctx;

	return 0;
}

int fr_channel_set_recv_request(fr_channel_t *ch, void *uctx, fr_channel_recv_callback_t recv_request)
{
	ch->end[TO_REQUESTOR].recv = recv_request;
	ch->end[TO_REQUESTOR].recv_uctx = uctx;
	return 0;
}

/** Send a channel to a responder
 *
 * @param[in] ch	The channel.
 * @return
 *	- <0 on error
 *	- 0 on success
 */
int fr_channel_signal_open(fr_channel_t *ch)
{
	fr_channel_control_t cc;

	cc.signal = FR_CHANNEL_SIGNAL_OPEN;
	cc.ack = 0;
	cc.ch = ch;

	return fr_control_message_send(ch->end[TO_RESPONDER].control, ch->end[TO_RESPONDER].rb, FR_CONTROL_ID_CHANNEL, &cc, sizeof(cc));
}

void fr_channel_debug(fr_channel_t *ch, FILE *fp)
{
	fprintf(fp, "to responder\n");
	fprintf(fp, "\tnum_signals sent = %" PRIu64 "\n", ch->end[TO_RESPONDER].num_signals);
	fprintf(fp, "\tnum_signals re-sent = %" PRIu64 "\n", ch->end[TO_RESPONDER].num_resignals);
	fprintf(fp, "\tnum_kevents checked = %" PRIu64 "\n", ch->end[TO_RESPONDER].num_kevents);
	fprintf(fp, "\tsequence = %"PRIu64"\n", ch->end[TO_RESPONDER].sequence);
	fprintf(fp, "\tack = %"PRIu64"\n", ch->end[TO_RESPONDER].ack);

	fprintf(fp, "to requestor\n");
	fprintf(fp, "\tnum_signals sent = %" PRIu64"\n", ch->end[TO_REQUESTOR].num_signals);
	fprintf(fp, "\tnum_kevents checked = %" PRIu64 "\n", ch->end[TO_REQUESTOR].num_kevents);
	fprintf(fp, "\tsequence = %"PRIu64"\n", ch->end[TO_REQUESTOR].sequence);
	fprintf(fp, "\tack = %"PRIu64"\n", ch->end[TO_REQUESTOR].ack);
}
