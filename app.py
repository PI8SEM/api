from flask import Flask, request, send_file, send_from_directory
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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    reports_dir = os.path.join(base_dir, 'reports')
    fileName = request.args.get("nome_arquivo") 
    return send_from_directory(reports_dir, fileName, as_attachment=True)

@app.route("/orquestrador", methods=["POST"])
def orquestrador():
    data = request.get_json()
    fileName = request.args.get("nome_arquivo")
    orquestrar_relatorio(data, fileName)
    return "200"

@app.route("/")
def index():
    # Serve the local index.html for quick testing
    return send_file("index.html")

if __name__ == "__main__":
    app.run(debug=True)
