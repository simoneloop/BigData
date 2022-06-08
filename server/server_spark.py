from http.server import HTTPServer,BaseHTTPRequestHandler
import json

HOST='localhost'
PORT=8080

class SparkServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.endswith('/func1'):
            self.send_response(200)
            self.send_header('content-type','application/json')
            self.end_headers()
            map={}
            map['hello']="world";
            response=json.dumps(map)
            self.wfile.write(response.encode())

def main():
    server_address=(HOST,PORT)
    server=HTTPServer(server_address,SparkServer)
    print('Server running on port %s' % PORT)
    server.serve_forever()

if __name__=='__main__':
    main()