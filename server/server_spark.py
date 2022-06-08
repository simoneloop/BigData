from http.server import HTTPServer,BaseHTTPRequestHandler
import json
from BigData.server.spark_core import prova

HOST='localhost'
PORT=8080
REQUEST_FILTER_ONE='func1'

ALL_FUNC=['func1','func2','func3']

def get_params(path):
    param=path.split('?')[1]
    params=param.split("&")
    res={}
    for p in params:
        tmp=p.split("=")
        if("[" in tmp[1] or "]" in tmp[1]):
            list=tmp[1].strip('][').split(',')
            res[tmp[0]]=list
        else:
            res[tmp[0]]=tmp[1]
    return res

def get_service_address(path):
    serviceAddress=path.split("?")[0]
    serviceAddress=serviceAddress.split("/")
    serviceAddress=serviceAddress[len(serviceAddress)-1]
    return serviceAddress

class SparkServer(BaseHTTPRequestHandler):
    def do_GET(self):
        params=get_params(self.path)
        service_address=get_service_address(self.path)
        if(service_address in ALL_FUNC):
            self.send_response(200)
            self.send_header('content-type', 'application/json')
            self.end_headers()

            if service_address =='func1':

                map={}
                map['hello']="world";
                response=json.dumps(map)
                self.wfile.write(response.encode())
            elif(service_address=="func2"):
                response=json.dumps(params)
                self.wfile.write(response.encode())
            elif(service_address=="func3"):
                response=json.dumps(prova())
        else:
            self.send_response(404)

def main():
    server_address=(HOST,PORT)
    server=HTTPServer(server_address,SparkServer)
    print('Server running on port %s' % PORT)
    server.serve_forever()

if __name__=='__main__':
    main()