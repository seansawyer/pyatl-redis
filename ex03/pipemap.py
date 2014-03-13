"""
Redis pipelines with asynchronous I/O.
"""
from gevent import monkey
monkey.patch_all()
import redis, zlib
from gevent.pool import Group
from time import time

HOST = '23.20.38.155'
NODES = [(HOST, port) for port in range(6380, 6385)]

def node(key):
    """Return the (host, port) tuple for a particular key."""
    i = zlib.crc32(key) % len(NODES)
    return NODES[i]

def client(key):
    """Return a redis.StrictRedis connected to the node for a key."""
    host, port = node(key)
    return redis.StrictRedis(host, port)

def pipeline(node):
    host, port = node
    r = redis.StrictRedis(host, port)
    return r.pipeline(transaction=False)

def execute_async(pipes):
    return Group().imap(lambda pipe: pipe.execute(), pipes)

def prepare_pipes():
    keys = ['ex03:pipe:{0}'.format(x) for x in range(25)]
    pipes = {node: pipeline(node) for node in NODES}
    for key in keys:
        pipes[node(key)].delete(key)
    for x in range(100):
        for key in keys:
            pipes[node(key)].incr(key)
    return pipes

def main():
    # Synchronous pipeline execution
    pipes = prepare_pipes()
    t0 = time()
    n_results = [len(pipe.execute()) for pipe in pipes.values()]
    t1 = time()
    print 'synchronous:', t1 - t0, 'seconds,', sum(n_results), 'results'
    # Aynchronous pipeline execution using Gevent
    pipes = prepare_pipes()
    t0 = time()
    n_results = [len(results) for results in execute_async(pipes.values())]
    t1 = time()
    print 'asynchronous:', t1 - t0, 'seconds,', sum(n_results), 'results'

if __name__ == '__main__':
    main()
