#
# loom.py a lightweight map-reduce using threads
# Copyright (C) 2019 Dennis Risen, Case Western Reserve University
#
""" To Do
Refactor parameter names
"""

import time
import traceback
from typing import Any, Callable, Iterable, List, Sequence

# direct threading is incompatible with threading
# When used in a Panda3d program, use direct.stdpy.threading instead
# import sys
# if 'direct.stdpy.threading' in sys.modules:
#     import direct.stdpy.threading as threading
#     print("Using direct.stdpy.threading as threading")
# else:
import threading


class Queue:
    """A bounded FIFO queue implemented using primitives in threading
    """

    def __init__(self, maxsize: int):
        self.put_semaphore = threading.Semaphore(maxsize)
        self.get_semaphore = threading.Semaphore(0)
        self.lock = threading.Lock()
        self.queue: List[Any] = []                 # the (FIFO) queue data is here

    def get(self) -> Any:
        """Pop the last entry from queue

        :return:
        """
        self.get_semaphore.acquire()    # wait for available entry
        self.lock.acquire()             # in case pop(0) is not atomic
        entry = self.queue.pop(0)
        self.lock.release()
        self.put_semaphore.release()    # its slot is now available
        return entry

    def put(self, val: Any):
        """Put val onto front of queue

        :param val:
        :return:
        """
        self.put_semaphore.acquire()    # wait for a slot in the queue
        self.lock.acquire()             # append might not be atomic
        self.queue.append(val)          # GIL-atomic add to the queue
        self.lock.release()
        self.get_semaphore.release()    # entry is available for get

    def qsize(self) -> int:
        """return length of the queue

        :return:
        """
        return len(self.queue)


def loom(producer: Iterable, mapfunc: Callable, consumer: Callable,
        mapfunc_kwargs: dict = None, consumer_kwargs: dict = None,
        verbose: int = 0, mappers: int = 1):
    """Creates a new thread for producer. Distributes each item that producer
    generates to one of mapper's threads that call mapfunc(item, **kwargs).
    Consumer, running in the calling thread, obtains the result of each
    mapfunc call by iterating its single argument. Functionally equivalent to
    def gen():
        for item in producer:
            yield mapfunc(item, **mapfunc_kwargs)
    consumer(gen(), **consumer_kwargs)


    Parameters:
        producer (iterable)		Source of items to map
        mapfunc (function)		mapfunc(item, **mapfunc_kwargs) is call w/ each item
        consumer (function)		called with iterator of the mapped results from mapfunc
        mapfunc_kwargs (dict)	optional additional keyword arguments to mapfunc
        consumer_kwargs (dict)	optional additional keyword arguments to consumer
        verbose (bool)			True to print performance info
        mappers (int)			number of mapper threads to start
    Results:
        (True, consumer(...))	on successful completion
        (False, error: str)		when there was an exception
    """

    if mapfunc_kwargs is None:
        mapfunc_kwargs = {}
    if consumer_kwargs is None:
        consumer_kwargs = {}

    # Each entry in a Queue is (Union[True, None], Object), where entry[0] is:
    # True --> request or result
    # None --> command to exit, or exit acknowledgment
    work_q = Queue(maxsize=2*mappers)   # queue from _producer to _mapper
    result_q = Queue(maxsize=2*mappers)  # queue from _mapper to _results
    counter_lock = threading.Lock()     # for locking +=, -= operations
    errmsg = []							# no errors, so far
    result_cnt = 0						# number of results returned from mapfunc

    def _producer():
        """
        Iterates the producer, queuing (True, item) to the work_q for each item.
        On exception or exhaustion of the producer, queues exit commands to mappers.
        """
        nonlocal errmsg, work_q
        try:
            for item in producer: 		# For each item from producer iterable
                if errmsg:  		    # exception occurred elsewhere. Stop producing.
                    break				# stop producing more items
                work_q.put((True, item))  # Queue item for mapfunc to process
            if verbose:
                print(f"producer StopIteration")
        except Exception as e:			# exception other than StopIteration
            traceback.print_exc()
            traceback.print_stack()
            errmsg.append(f"{e} in {getattr(producer, '__name__', 'producer')}")
        for i in range(mappers): 		# tell each mapper to exit
            work_q.put((None, None))

    def _results():
        """Generator of each item from result_q, while mapper(s) are still active"""
        nonlocal result_q, mapper_cnt, result_cnt
        while mapper_cnt > 0:
            item: Sequence = result_q.get()
            if item[0]:
                counter_lock.acquire()  # += is not atomic
                result_cnt += 1
                counter_lock.release()
                if verbose and result_cnt % 5000 == 0:
                    print(f"{result_cnt} items processed in {time.time()-start_time} seconds")
                yield item[1]
            else:						# a mapper has exited?
                counter_lock.acquire()  # because -= is not atomic
                mapper_cnt -= 1			#
                counter_lock.release()

    def _mapper():
        nonlocal errmsg, result_q, work_q
        while True:
            item = work_q.get() 		# next item of work
            valid = item[0]
            if valid:					# normal data item?
                try:
                    x = mapfunc(item[1], **mapfunc_kwargs)
                except Exception as e:  # exception in mapfunc
                    traceback.print_exc()
                    traceback.print_stack()
                    errmsg.append(f"{e} in {getattr(mapfunc,'__name__','mapfunc')}")
                else:
                    result_q.put((True, x))
            else:						# told to exit?
                result_q.put((None, None))  # I'm exiting
                break

    # Start the producer to queue objects to work_q
    t = threading.Thread(name='producer', target=_producer)
    t.start()
    threads = [t]
    start_time = time.time()
    if verbose:
        print(f"mappers starting")

    # Start mappers, which each map input items from work_q to result_q
    mapper_cnt = mappers 	# the number of mapper exits not yet seen by _results
    for i in range(mappers):
        t = threading.Thread(name='mapper'+str(i), target=_mapper)
        t.start()
        threads.append(t)
    # mapper_cnt = mappers 	# the number of mapper exits not yet seen by _results

    # call consumer with results generator
    err = None
    result = None
    try:
        result = consumer(_results(), **consumer_kwargs)
    except KeyboardInterrupt:
        err = 'KeyboardInterrupt'
    except SystemExit:
        err = 'SystemExit'
    except Exception as e:
        traceback.print_exc()
        traceback.print_stack()
        err = str(e)
    if err is not None:					# an exception?
        errmsg.append(f" {err} in {getattr(consumer,'__name__','consumer')}")
    # Consumer and _results have exited.
    # unblock _producer and _mapper if consumer didn't consume all work
    for item in _results():			    # Consume any remaining results
        print(f"Consumed leftover {item}")
    # The producer and mappers have exited, and all work has been consumed
    for t in threads:					# join all of the threads together
        t.join()
    if verbose:
        delta_secs = time.time()-start_time
        print(f"Completed {result_cnt} objects in {delta_secs:5.3f} seconds.")
    if work_q.qsize() > 0 or result_q.qsize() > 0:  # some data left unprocessed?
        print(f"ERROR: len(work_q)={work_q.qsize()}, len(result_q)={result_q.qsize()}")
    if not errmsg:
        return True, result
    else:
        return False, errmsg
