import sys
assert sys.version_info >= (3, 5), 'Python version >=3.5 is required to run this script'

import csv
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import threading
import json
import requests
from pprint import pprint
import datetime

class RuleNotFound(Exception):
    pass

#open log file
logfile = open('log.txt','w',buffering=1)

# Check if the first line of rules.csv contains space characters
with open ('rules.csv',newline='') as csvfile:
    firstline=csv.reader(csvfile,delimiter=',', quotechar='"')
    for word in firstline:
        if ' ' in word:
            print('The rules.csv should not contain space characters')
            print('There is a space character at:',word)
            sys.exit()

rules=[]
# Read the whole rules.csv again
with open ('rules.csv',newline='') as csvfile:
    lines=csv.DictReader(csvfile,delimiter=',', quotechar='"')
    for line in lines:
        rules.append(line)

class MyServer(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def do_POST(self):
        print('===BEGIN REQUEST ',end='',file=logfile)
        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),file=logfile)
        keys=('First','Second','Third','Fourth','Fifth','Sixth','Seventh','Eighth','Ninth','Tenth')
        request_length = int(self.headers['content-length'])
        r=''
        response_length=0
        result=''
        if not self.headers['content-type']:
            print('Content-Type header is missing',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)
            return
        elif self.headers['content-type'] != 'application/json':
            print('Content-Type should be application/json',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)            
            return
        pd0=self.rfile.read(request_length)
        pd1=''
        try:
            pd1= pd0.decode('utf-8')
        except:
            print('Unicode Decoding Error',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)            
            return
        try:
            loaded = json.loads(pd1)
        except json.decoder.JSONDecodeError:
            print('JSON Parsing Error',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)
            return
        try:
            current={}
            for d in loaded['DATA']:
                current['DeviceID']=loaded['IM']
                current['ParameterName']=d[0]                
                current['SlaveID']=d[1]
                current['ParameterAddress']=d[3]
                
#                 ----------------------------------------------------------------------------------------------
                matched=True
                for r in range(len(rules)):
                    matched=True
                    print('The rule is:',rules[r])
                    if current[r] ==rules[r]:
                        continue
                    else:
                        matched=False
                        break
#                         ----------------------------------------------------------------------------------------------------
                if matched:
                    print('But the json contains:',current)
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
            print('None of the rules in rules.csv match with the current json. Discarding the current json',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)            
            return
            
        except Exception as e:
            print('500, internal error',file=logfile)
            self.finish()
            self.connection.close()
            print('END REQUEST===\n',file=logfile)
            return
        print('Post request submitted successfully to http://localhost:8086/write?db=mydb\n',file=logfile)
        self.finish()
        self.connection.close()
        print('END REQUEST===\n',file=logfile)        
        return
    
    def do_GET(self):
        print('GET Method not allowed.  Allowed methods: POST',file=logfile)
        self.finish()
        self.connection.close()
        print('END REQUEST===\n',file=logfile)        
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
