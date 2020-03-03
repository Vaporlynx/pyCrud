from http.server import HTTPServer, BaseHTTPRequestHandler

import redis, json

redisInstance = redis.Redis(host="127.0.0.1", port=31337)

# curl -i -X POST 127.0.0.1:1337/instantiate/fruits
# curl -i -X POST --data '{"culinaryVegetable": false, "berry": true, "color": "orange"}' 127.0.0.1:1337/entry/fruits/orange
# curl -i -X POST --data '{"culinaryVegetable": false, "berry": true, "color": "red"}' 127.0.0.1:1337/entry/fruits/strawberry
# curl -i -X PATCH --data '{"sweet": true}' 127.0.0.1:1337/entry/fruits/strawberry
# curl -i 127.0.0.1:1337/entry/fruits/orange
# curl -i 127.0.0.1:1337/entry/fruits/strawberry
# curl -i -X DELETE 127.0.0.1:1337/entry/fruits/orange
# curl -i 127.0.0.1:1337/entry/fruits/orange

# TODO: implement rename.  What method to use?
# PUT?  PATCH with some query param set in the url?
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith("/instantiate"):
            split = self.path.split("/")
            table = split[len(split) - 1]
            if redisInstance.exists(table):
                self.send_error(403, "TABLE_ALREADY_EXISTS")
                return

            redisInstance.hset(table, "", "")
            self.send_response(200)
            self.end_headers()
            return

        if self.path.startswith("/entry"):
            split = self.path.split("/")
            table = split[len(split) - 2]
            entry = split[len(split) - 1]

            if redisInstance.exists(table):
                if redisInstance.hexists(table, entry):
                    self.send_error(403, "ENTRY_ALREADY_EXISTS")
                    return

                content_length = int(self.headers["Content-Length"])
                body = self.rfile.read(content_length)
                if len(body):
                    decoded = body.decode("utf-8")
                    redisInstance.hset(table, entry, decoded)
                    self.send_response(200)
                    self.end_headers()
                    return
                
                self.send_error(400, "EMPTY_OBJECT")

        self.send_error(404, "TABLE_NOT_FOUND")

    def do_PATCH(self):
        if self.path.startswith("/entry"):
            split = self.path.split("/")
            table = split[len(split) - 2]
            entry = split[len(split) - 1]

            if redisInstance.exists(table):
                if redisInstance.hexists(table, entry):
                    content_length = int(self.headers["Content-Length"])
                    body = self.rfile.read(content_length)
                    if len(body):
                        existingEntry = redisInstance.hget(table, entry)
                        parsedEntry = json.loads(existingEntry)

                        decoded = body.decode("utf-8")
                        parsedupdate = json.loads(decoded)
                        updateKeys = parsedupdate.keys()
                        for key in updateKeys:
                            parsedEntry[key] = parsedupdate[key]

                        redisInstance.hset(table, entry, json.dumps(parsedEntry))
                        self.send_response(200)
                        self.end_headers()
                        return
                    self.send_error(400, "EMPTY_OBJECT")
        
        self.send_error(404)

    def do_DELETE(self):
        if self.path.startswith("/entry"):
            split = self.path.split("/")
            table = split[len(split) - 2]
            entry = split[len(split) - 1]
            redisInstance.hdel(table, entry)
            self.send_response(200)
            self.end_headers()
            return

        self.send_error(404)

    def do_GET(self):
        if self.path.startswith("/entry"):
            split = self.path.split("/")
            table = split[len(split) - 2]
            entry = split[len(split) - 1]
            if redisInstance.exists(table):
                if redisInstance.hexists(table, entry):
                    existingEntry = redisInstance.hget(table, entry)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(existingEntry)
                    return
   
        self.send_error(404)

server = HTTPServer(("localhost", 1337), handler)

server.serve_forever()