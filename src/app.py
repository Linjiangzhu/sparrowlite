from flask import Flask,render_template, request
import controller, time

app = Flask(__name__)

@app.route("/")
def indexHanlder():
    return render_template("index.html")

@app.route("/search")
def searchPageHandler():
    query = request.args.get("q")
    if query == None:
        query = ""
    start = time.time()
    result = controller.getQuery(query)
    end = time.time()
    query_time = round(end - start, 2)
    length = len(result)
    return render_template("search.html", query=query, result = result, length=length, query_time = query_time)

@app.route("/cache")
def cachedPageHandler():
    query = request.args.get("q")
    html_page = controller.getCachedPage(query)
    return html_page