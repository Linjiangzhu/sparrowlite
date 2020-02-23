from flask import Flask,render_template, request
import controller

app = Flask(__name__)

@app.route("/")
def indexHanlder():
    return render_template("index.html")

@app.route("/search")
def searchPageHandler():
    query = request.args.get("q")
    if query == None:
        query = ""
    return render_template("search.html", query=query)

@app.route("/cache")
def cachedPageHandler():
    query = request.args.get("q")
    html_page = controller.getTestCachePage()
    return html_page