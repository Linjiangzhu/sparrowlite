# Sparrow Lite -- a micro search engine
This project is established for UCI CS121 final assignment
## Build and run
you need to install Flask in your python environment first, read [Flask Official Installation Guide](https://flask.palletsprojects.com/en/1.1.x/installation/) for more information.
To run the application you are required to set your `FLASK_APP` environment variable.
For Windows:

```i
cd src
set FLASK_APP=main.py
```
For macos or Unix-like:
```bash
cd src
export FLASK_APP=main.py
```
To run the program:
```
flask run --host=127.0.0.1 --port=3000
```
The program will listen and serve the application at localhost on port 3000. Visit http://localhost:3000 in your browser to see the web page.
## Goals when established:
- web UI based on Flask using template
- python as backend
## Stage 1
- You need to implement a inverted index