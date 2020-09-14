import sys
assert sys.version_info >= (3, 5), 'Python version >=3.5 is required to run this script'

import csv
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
import json
import requests
from pprint import pprint

class RuleNotFound(Exception):
    pass


# Check if the first line of rules.csv contains space characters
with open ('rules.csv',newline='') as csvfile:
    firstline=csv.reader(csvfile,delimiter=',', quotechar='"')
    for word in firstline:
        if ' ' in word:
            print('The rules.csv should not contain space characters',file=sys.stderr)
            print('There is a space character at:',word,file=sys.stderr)
            sys.exit()

rules=[]
# Read the whole rules.csv again
with open ('rules.csv',newline='') as csvfile:
    lines=csv.DictReader(csvfile,delimiter=',', quotechar='"')
    for line in lines:
        rules.append(line)

class MyServer(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
  
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.send_header('access-control-allow-origin', '*')
        self.send_header('access-control-allow-headers', 'content-type')
        self.send_header("Content-Length", 0)                
        self.end_headers()
        self.wfile.write(''.encode('utf-8'))        
        return

    def do_POST(self):
        keys=('First','Second','Third','Fourth','Fifth','Sixth','Seventh','Eighth','Ninth','Tenth')
        request_length = int(self.headers['content-length'])
        r=''
        response_length=0
        result=''
        if not self.headers['content-type']:
            self.send_response(400) 
            self.send_header("Content-type", "text/plain")            
            r=json.dumps('Content-Type header is missing')            
            self.send_header("Content-Length", len(r))
            self.end_headers()

            self.wfile.write(r.encode('utf-8'))
            return
        elif self.headers['content-type'] != 'application/json':
            self.send_response(400)
            self.send_header("Content-type", "text/plain")            
            r=json.dumps('Content-Type should be application/json')
            self.send_header("Content-Length", len(r))
            self.end_headers()
            self.wfile.write(r.encode('utf-8'))
            return
        pd0=self.rfile.read(request_length)
        pd1=''
        try:
            pd1= pd0.decode('utf-8')
        except:
            r=json.dumps('Unicode Decoding Error1')
            self.send_header("Content-Length", len(r))
            self.end_headers()
            self.send_response(400)
            self.wfile.write(r.encode('utf-8'))
            return
        try:
            loaded = json.loads(pd1)
        except json.decoder.JSONDecodeError:
            self.send_response(400)            
            r=json.dumps('JSON Parsing Error')
            self.send_header("Content-Length", len(r))
            self.end_headers()
            self.wfile.write(r.encode('utf-8'))
            return
        try:
            current={}
            for d in loaded['DATA']:
                current['DeviceID']=loaded['IM']
                current['ParamaterName']=d[0]                
                current['SlaveID']=d[1]
                current['ParamaterAddress']=d[3]
                matched=False
                for r in rules:
                    if current == r:
                        matched=True
                        break
                if not matched:
                    raise RuleNotFound
                result =  d[4]
                result_array=[]
                for i in range(6, len(result)-4, 4):
                    dec=int( result[i:i+4] , 16)
                    result_array.append(dec)
                z=tuple(zip(keys,result_array))
                target_data='ModbusData '
                for a,b in z:
                    target_data = target_data + a +'=' + str(b) + ','
                # remove the last comma
                target_data=target_data[:-1]
                print(target_data)
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
                s=requests.post('http://localhost:8086/write?db=mydb',data=target_data,headers=headers)
                print(s.headers)
                print(s.reason)
        except RuleNotFound as e:
            self.send_response(406)            
            r=json.dumps('None of the rules in rules.csv match with the current json. Discarding the current json')
            self.send_header("Content-Length", len(r))
            self.end_headers()
            self.wfile.write(r.encode('utf-8'))
            return
            
        except Exception as e:
            self.send_response(500)            
            r=json.dumps(e.__class__.__name__)
            self.send_header("Content-Length", len(r))
            self.end_headers()
            self.wfile.write(r.encode('utf-8'))
            return
        self.send_response(200)
        e='Post request sent to http://172.104.176.90:8086/write?db=mydb\n'
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", len(e))                
        self.end_headers()
        self.wfile.write(e.encode('utf-8'))            
        return

    
    def do_GET(self):
        self.send_response(405)        
        self.send_header("Content-type", "text/html")
        r = json.dumps('GET Method not allowed.  Allowed methods: OPTIONS, POST')
        self.send_header("Content-Length", len(r))        
        self.end_headers()
        self.wfile.write(r.encode('utf-8'))
        return
        
class ThreadingSimpleServer(ThreadingMixIn,HTTPServer):
    pass

if __name__ == "__main__":
    webServer = ThreadingSimpleServer(('', 3000), MyServer)
    #webServer.socket = ssl.wrap_socket(webServer.socket, keyfile='./privkey.pem',certfile='./certificate.pem', server_side=True)
#    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
