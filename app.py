from flask import Flask, request, send_file
from flask_cors import CORS
import json
from gerar_relatorio import gerar_pdf
from v2_reportGenerate import orquestrar_relatorio
import os

app = Flask(__name__)
CORS(app) 

@app.route("/receber", methods=["POST"])
def receber_dados():
    dados = request.get_json()
    with open("dados_telemetria.json", "w") as f:
        json.dump(dados, f, indent=4)
    return {"status": "ok"}

@app.route("/relatorio", methods=["GET"])
def relatorio():
    fileName = request.args.get("./reports/{fileName}")   
    return send_file(fileName, as_attachment=True)

@app.route("/orquestrador", methods=["POST"])
def orquestrador():
    data = request.get_json()
    fileName = request.args.get("nome_arquivo")
    orquestrar_relatorio(data, fileName)

@app.route("/")
def index():
    # Serve the local index.html for quick testing
    return send_file("index.html")

if __name__ == "__main__":
    app.run(debug=True)
