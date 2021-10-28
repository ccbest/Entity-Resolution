import socketserver
import time
from random import choice
import json

class AlphaTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print('AlphaTCPHandler')
        alphabets = list('abcdefghikjklmnopqrstuvwxyz')

        try:
            while True:
                asdd = {"test": "val"}
                s = json.dumps(f'{asdd}')
                b = bytes(s + '\n', 'utf-8')
                self.request.sendall(b)
                time.sleep(1)
        except BrokenPipeError:
            print('broken pipe detected')

if __name__ == '__main__':
    host = '0.0.0.0'
    port = 301

    server = socketserver.TCPServer((host, port), AlphaTCPHandler)
    print(f'server starting {host}:{port}')
    server.serve_forever()