import redis, time

def main():
    r = redis.StrictRedis()
    while True:
        msg = 'Hello?'
        print msg
        r.publish('lurkers-only', msg)
        time.sleep(5)

if __name__ == "__main__":
    main()
