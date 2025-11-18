from flask import Flask, request, send_file, send_from_directory, jsonify
from flask_cors import CORS

import json
import os

from gerar_relatorio import gerar_pdf
from v2_reportGenerate import orquestrar_relatorio
from agente import callAgent
from get_api_data import get_api_data


from analise_tensao_rms import analisar_dados_json as analisar_tensao_rms
from analise_corrente_rms import analisar_corrente_json as analisar_corrente_rms
from analise_potencia_ativa_reativa import analisar_potencia_json as analisar_potencia_ativa_reativa
from analise_demanda_perfil import analisar_demanda_json as analisar_demanda_perfil

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

# Definição da rota da API
@app.route("/agente", methods=["POST"])
def agente():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Nenhum dado JSON recebido"}), 400

    print(f"Dados recebidos no endpoint /agente: {data}")

    # Chama a função importada para encaminhar os dados ao webhook
    webhook_response, error_message, status_code = callAgent(data)

    if error_message:
        # Se callAgent retornou um erro, informa quem chamou
        return jsonify({"error": "Falha ao processar agente", "details": error_message}), status_code

    # Se deu tudo certo, retorna a resposta do webhook
    try:
        # Tenta retornar o JSON da resposta do webhook
        return jsonify(webhook_response.json()), status_code
    except json.JSONDecodeError:  # <-- 2. Capture a exceção padrão
        # Se o webhook não retornou JSON, retorna o texto
        return webhook_response.text, status_code

@app.route("/get_api_data", methods=["GET"])
def get_api_data_route():
    """
    Rota do Flask que chama a função get_api_data 
    e retorna os dados como uma resposta JSON.
    """
    # Chama a função importada
    data, status_code = get_api_data()
    
    # Usa jsonify para formatar a resposta corretamente
    return jsonify(data), status_code


@app.route("/analise_tensao_rms", methods=["POST"])
def analise_tensao_rms():
    data = request.get_json()
    resultado = analisar_tensao_rms(data)
    return resultado

@app.route("/analise_corrente_rms", methods=["POST"])
def analise_corrente_rms():
    data = request.get_json()
    resultado = analisar_corrente_rms(data)
    return resultado

@app.route("/analise_potencia_ativa_reativa", methods=["POST"])
def analise_potencia_ativa_reativa():
    data = request.get_json()
    resultado = analisar_potencia_ativa_reativa(data)
    return resultado

@app.route("/analise_demanda_perfil", methods=["POST"])
def analise_demanda_perfil():
    data = request.get_json()
    resultado = analisar_demanda_perfil(data)
    return resultado

@app.route("/")
def index():
    # Serve the local index.html for quick testing
    return send_file("index.html")

if __name__ == "__main__":
    app.run(debug=True)
