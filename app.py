from flask import Flask, render_template
from flask_cors import CORS, cross_origin

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@cross_origin()
@app.route('/')
def index():
    return render_template('index.html')


app.run(host='0.0.0.0', port=8080)
