"""
Prove that Redis strings are binary-safe using a JPEG.
"""
import BaseHTTPServer, redis

key = 'ty:jpeg'

class RedisHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        """Serve a JPEG image to all GET requests."""
        r = redis.StrictRedis()
        data = r.get(key)
        self.send_response(200)
        self.send_header('Content-type', 'image/jpeg')
        self.end_headers()
        self.wfile.write(data)

def main():
    with open('ty.jpg', 'rb') as f:
        jpeg = f.read()
    redis.StrictRedis().set(key, jpeg)
    httpd = BaseHTTPServer.HTTPServer(('', 8000), RedisHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    main()
