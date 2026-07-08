import urllib.parse
import urllib.request

urls = [
    "file:///tmp/test.zip",
    "file:///C:/Users/runneradmin/test.zip",
    "file://C:/Users/runneradmin/test.zip",  # Edge case
]

for url in urls:
    if url.startswith("file://"):
        # The original code before my refactor did: `local_path = url[7:]`
        original_slice = url[7:]

        # New approach attempting to use url2pathname
        # According to Python docs, url2pathname expects the path component of a URL
        # But for Windows, file:///C:/... means url2pathname should get /C:/... -> C:\...
        # and file://... means url2pathname should get ...
        if url.startswith("file:///"):
            path_part = url[7:]  # Windows: C:/... Unix: /tmp/...
        elif url.startswith("file://"):
            path_part = url[7:]

        local_path = urllib.request.url2pathname(urllib.parse.unquote(path_part))

        print(f"URL: {url}")
        print(f"Orig: {original_slice}")
        print(f"New: {local_path}")
        print("---")
