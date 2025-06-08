from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
from lsm_db import SimpleLSMDB

db_instance = None

class DBRequestHandler(BaseHTTPRequestHandler):
    """Manipula requisições HTTP do banco."""
    def do_GET(self):
        """Processa requisições GET."""
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/get':
            params = urllib.parse.parse_qs(parsed.query)
            key = params.get('key', [None])[0]
            if key is None:
                self.send_error(400, 'Missing key')
                return
            value = db_instance.get(key)
            if value is None:
                value = ''
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(value.encode())
        else:
            self.send_error(404)

    def do_POST(self):
        """Processa requisições POST."""
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path == '/put':
            key = params.get('key', [None])[0]
            value = params.get('value', [None])[0]
            if key is None or value is None:
                self.send_error(400, 'Missing key or value')
                return
            db_instance.put(key, value)
            self.send_response(200)
            self.end_headers()
        elif parsed.path == '/delete':
            key = params.get('key', [None])[0]
            if key is None:
                self.send_error(400, 'Missing key')
                return
            db_instance.delete(key)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_error(404)


def run_server(db_path, host='localhost', port=8000):
    """Inicializa servidor HTTP para uma réplica."""
    global db_instance
    db_instance = SimpleLSMDB(db_path=db_path)
    server = HTTPServer((host, port), DBRequestHandler)
    print(f'Node server running on {host}:{port}')
    server.serve_forever()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Run SimpleLSM node server')
    parser.add_argument('--path', required=True, help='Database path')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    run_server(args.path, args.host, args.port)
