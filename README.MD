# Sparrow Lite -- a micro search engine
This project is established for UCI CS121 final assignment
## Project Structure
```
src
├── app.py
├── build.py
├── config.ini
├── controller.py
├── model.py
├── packages
│   └── requirements.txt
├── static
│   ├── index.css
│   ├── logo.png
│   └── search.css
├── templates
│   ├── index.html
│   └── search.html
└── util
    ├── indexBuilder.py
    ├── simhashIndex.py
    └── textProcessor.py
```
## Goals and project progress
### Stage 1
- [x] Implement a inverted index
- [x] posting include tf-idf score
### Stage 2
- [x] multi query
### Stage 3
- [x] token stemming
- [x] weight of words in title,\<strong>, and heading
- [x] less than 300ms response
- [x] a complete search engine
### Extra credit
- [x] duplicate and near duplicate pages
- [x] page rank
- [ ] 2-gram/3-gram indexing
- [ ] posting with word position
- [ ] anchor words
- [x] Web or GUI interface
## Build and run
Download the processed data files [from here](https://github.com/Linjiangzhu/sparrowlite/releases/download/0.0.2-alpha/DATA.zip), unzip to the same directory where you place this project. The file structure should be look like this:
```
sparrowlite
├── src
├── DATA <--drop it here
└── README.MD
```

You need to specify where the source data directory and where to put processed data in `src/config.ini`

```ini
[DATABASE]
WEBSITES_DIR = ../DEV/ # dir where your raw source files locate, if you use built data file I provided, no need to change this one
DATABASE_DIR = ../DATA/    # output dir of your data   
MERGE_CHUNK  = YES         # do not change this line
```



Install dependencies:

Python 2
```bash
pip install -r src/package/requirements.txt
```
Python 3
```bash
pip3 install -r src/package/requirements.txt
```

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
## Known Issues
- [x] need to rewrite the function to merge partial data files using heapq
- [x] ugly retrieval result styling
- [x] stemming
- [x] use config.ini to set crawled website directory and output directory
- [x] implement forword idx of inverted idx for retrieval docuements
- [x] return just url for stage 2, not path
- [x] flx disk seek difference on windows and mac/unix-like
- [x] implement complex tuple intersection algorithm
- [x] slow or false implementation of cosine similarity
- [x] use Simhash to remove near duplicates
- [x] fix "," ValueError when parsing docid csv
- [x] implement inward and outward link graph