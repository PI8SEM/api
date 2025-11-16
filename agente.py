import requests

# Dicionário de ambientes
ambiente = {
    "desenvolvimento": "https://n8n-n8n.wnmocf.easypanel.host/webhook-test/create_document",
    "producao": "https://n8n-n8n.wnmocf.easypanel.host/webhook/create_document"
}

def callAgent(data):
    """
    Função que extrai os dados e faz a chamada POST 
    para o webhook da n8n.
    """
    try:
        # Extrai os dados do JSON recebido
        inicioReport = data["inicioReport"]
        fimReport = data["fimReport"]
        cliente = data["cliente"]
        unidade = data["unidade"]
        analise = data["analise"]

    except KeyError as e:
        # Retorna um erro se uma chave estiver faltando
        print(f"ERRO: Chave ausente nos dados: {e}")
        # Retorna (None, mensagem_de_erro, status_code)
        return None, f"Chave ausente: {e}", 400

    # Monta o corpo da requisição para o webhook
    body = {
        "inicioReport": inicioReport,
        "fimReport": fimReport,
        "cliente": cliente,
        "unidade": unidade,
        "analise": analise
    }

    try:
        # Faz a requisição POST para o ambiente de desenvolvimento
        url = ambiente["desenvolvimento"]
        response = requests.post(url, json=body)
        
        # Lança um erro para status HTTP ruins (4xx ou 5xx)
        response.raise_for_status()
        
        # Retorna o objeto de resposta, nenhuma mensagem de erro, e o status da resposta
        return response, None, response.status_code

    except requests.exceptions.RequestException as e:
        # Captura qualquer erro de rede ou HTTP
        print(f"ERRO ao chamar o webhook: {e}")
        # Retorna (None, mensagem_de_erro, status_code_http_se_existir_ou_500)
        status_code = e.response.status_code if e.response is not None else 500
        return None, str(e), status_code