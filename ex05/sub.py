import redis, time

def main():
    r = redis.StrictRedis()
    pubsub = r.pubsub()
    pubsub.subscribe(['lurkers-only'])
    # Lurk for a bit...
    n_msgs = 0
    for msg in pubsub.listen():
        n_msgs += 1
        print msg
        if n_msgs == 10:
            pubsub.unsubscribe()
            break

if __name__ == "__main__":
    main()
