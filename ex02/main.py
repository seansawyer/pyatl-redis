"""
Demonstrate consistent hashing for distributing keys across multiple
Redis instances.
"""
import redis, zlib

# Redis nodes as (host, port) tuples
NODES = [
    ('localhost', 6380),
    ('localhost', 6381),
]

def node(key):
    """Return the (host, port) tuple for a particular key."""
    i = zlib.crc32(key) % len(NODES)
    return NODES[i]

def client(key):
    """Return a redis.StrictRedis connected to the node for a key."""
    host, port = node(key)
    return redis.StrictRedis(host, port)

def main():
    keys = ['foo', 'bar', 'baz']
    for key in keys:
        print key, 'hashes to', node(key)
        value = key[::-1] # reverse of the key
        client(key).set(key, value)
    for key in keys:
        print key, '->', client(key).get(key)

if __name__ == '__main__':
    main()
