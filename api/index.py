from http.server import BaseHTTPRequestHandler
import json
import os
import time
from openai import OpenAI
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import traceback

from .scraper_to_vector_store import get_knowledge_source

load_dotenv()

CLIENT = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

MAX_REQUESTS_PER_HOUR = 120
RATE_LIMIT_RESET_INTERVAL = 3600

request_count = 0
last_reset_time = time.time()


def check_rate_limit():
    """Check if the current request exceeds the rate limit"""
    global request_count, last_reset_time

    current_time = time.time()

    if current_time - last_reset_time > RATE_LIMIT_RESET_INTERVAL:
        request_count = 0
        last_reset_time = current_time

    if request_count >= MAX_REQUESTS_PER_HOUR:
        return False

    request_count += 1
    return True


class handler(BaseHTTPRequestHandler):
    def extract_query_params(self):
        parsed_url = urlparse(self.path)
        base_path = parsed_url.path
        query_params = parse_qs(parsed_url.query)
        params = {k: v[0] for k, v in query_params.items()}
        return base_path, params

    def do_GET(self):
        base_path, _ = self.extract_query_params()

        if base_path == '/':

            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            html_content = """
            <!DOCTYPE html>
            <html>
            <head>
                <title>Vector Store API</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }
                    h1 { color: #333; }
                    h2 { color: #555; margin-top: 25px; }
                    pre { background-color: #f5f5f5; padding: 15px; border-radius: 5px; overflow-x: auto; }
                    .container { max-width: 800px; margin: 0 auto; }
                    .example { margin-top: 20px; }
                    code { background-color: #f0f0f0; padding: 2px 4px; border-radius: 3px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Vector Store API</h1>
                    <p>This API allows you to create OpenAI vector stores from web content.</p>
                    
                    <h2>Available Endpoints:</h2>
                    <ul>
                        <li><code>POST /api/vector-store</code> - Create a new vector store from URLs</li>
                    </ul>
                    
                    <h2>Example Usage:</h2>
                    <div class="example">
                        <h3>Create Vector Store</h3>
                        <p>To create a new vector store, send a POST request to <code>/api/vector-store</code> with the following JSON body:</p>
                        <pre>
{
  "urls": ["https://example.rit.edu", "https://anotherexample.rit.edu"],
  "name": "Your Vector Store Name"
}
                        </pre>
                        
                        <p>Or with custom refresh times:</p>
                        <pre>
{
  "urls": {
    "https://example.rit.edu": "1 day", 
    "https://anotherexample.rit.edu": "1 week"
  },
  "name": "Your Vector Store Name"
}
                        </pre>
                        
                        <p>Response:</p>
                        <pre>
{
  "vector_store_id": "vs_abc123..."
}
                        </pre>
                    </div>
                    
                    <h2>Rate Limits:</h2>
                    <p>Vector store creations are limited to <strong>120</strong> requests per hour. If you exceed this 
                    limit, you'll receive a 429 response with a Retry-After header. Cached results or cases where 
                    the vector store doesn't have to be remade don't count towards this limit</p>
                </div>
            </body>
            </html>
            """
            self.wfile.write(html_content.encode())
            return
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response_data = {"error": f"Endpoint {self.path} not found."}
            self.wfile.write(json.dumps(response_data).encode())
            return

    def do_POST(self):
        base_path, _ = self.extract_query_params()

        if base_path == '/api/vector-store' or base_path == '/api/vector-store/' or base_path == '/vector-store' or base_path == '/vector-store/':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8')

            response_data = {}
            status_code = 200

            if not check_rate_limit():
                status_code = 429
                response_data = {"error": "Rate limit exceeded. Please try again later."}
            else:
                try:
                    request_data = json.loads(post_data)

                    if 'urls' not in request_data or 'name' not in request_data:
                        status_code = 400
                        response_data = {"error": "Missing required fields: 'urls' and 'name' are required"}
                    else:
                        urls = request_data['urls']
                        name = request_data['name']

                        try:
                            vector_store_id, made_openai_calls = get_knowledge_source(urls, name)

                            if not made_openai_calls and request_count > 0:
                                request_count -= 1

                            if vector_store_id:
                                print("ID is: " + vector_store_id)
                                response_data = {
                                    'vector_store_id': vector_store_id
                                }
                            else:
                                status_code = 500
                                response_data = {"error": "Failed to create vector store"}
                        except Exception as e:
                            print(f"Error in get_knowledge_source: {str(e)}")
                            traceback.print_exc()
                            status_code = 500
                            response_data = {"error": f"Processing error: {str(e)}"}
                except json.JSONDecodeError:
                    status_code = 400
                    response_data = {"error": "Invalid JSON in request body"}
                except Exception as e:
                    print(f"General error: {str(e)}")
                    traceback.print_exc()
                    status_code = 500
                    response_data = {"error": f"An error occurred: {str(e)}"}
        else:
            status_code = 404
            response_data = {"error": f"Endpoint {self.path} not found"}

        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        if status_code == 429:
            self.send_header('Retry-After', str(RATE_LIMIT_RESET_INTERVAL))
        self.end_headers()
        self.wfile.write(json.dumps(response_data).encode())
        return

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        return
