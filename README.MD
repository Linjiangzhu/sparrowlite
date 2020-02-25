# Sparrow Lite -- a micro search engine
This project is established for UCI CS121 final assignment
## Build and run
A data file is required before you run the flask application. You can either put a complete inverted index build in `./DATA/` or run `python build.py` to directly build inverted index data file from source JSON.

To install Flask in your python environment, read [Flask Official Installation Guide](https://flask.palletsprojects.com/en/1.1.x/installation/) for more information.

To run the application you are required to set your `FLASK_APP` environment variable.

For Windows:

```batch
cd src
set FLASK_APP=main.py
```
For macos or Unix-like:
```bash
cd src
export FLASK_APP=main.py
```
Start flask application
```
flask run --host=127.0.0.1 --port=3000
```
The program will listen and serve the application at localhost on port 3000. Visit http://localhost:3000 in your browser to see the web page.
## Goals and project progress
### Stage 1
- [x] Implement a inverted index
- [x] posting include tf-idf score
### Stage 2
- [ ] multi query
### Stage 3
- [ ] token stemming
- [ ] stop words
- [ ] weight of words in title,\<strong>, and heading
- [ ] posting include tf-idf score
- [ ] a complete search engine
### Extra credit
- [ ] duplicate and near duplicate pages
- [ ] page rank
- [ ] 2-gram/3-gram indexing
- [ ] posting with word position
- [ ] anchor words
- [x] Web or GUI interface