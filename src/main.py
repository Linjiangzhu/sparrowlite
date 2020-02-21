from flask import Flask, render_template
app = Flask(__name__)

@app.route("/")
def indexHanlder():
    return render_template("index.html")

@app.route("/search")
def searchPageHandler():
    return render_template("search.html")