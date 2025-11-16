import requests


def get_api_data():
    """
    Busca dados da API CircuitSense, enviando cookies de autenticação
    para contornar o firewall de anti-bot (Akamai).
    """
    url = "https://oracleapex.com/ords/projeto_8/Circuitsense/dados"
    
    # --- INÍCIO DA MUDANÇA ---
    
    # Cole o cookie completo do seu comando curl aqui
    # ATENÇÃO: Este cookie VAI EXPIRAR.

    # Adicionamos os cabeçalhos que o Postman usa
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        "accept" : '*/*/',
    }
    
    # --- FIM DA MUDANÇA ---

    try:
        
        # Passa os headers na requisição
        response = requests.get(url, headers=headers, timeout=20) 
        response.raise_for_status()  
        json_data = response.json()
        return json_data, 200

    # ... (o resto do seu tratamento de erro continua igual) ...
    except requests.exceptions.Timeout as err_timeout:
        logging.error(f"Timeout Error: A requisição demorou mais de 20 segundos. {err_timeout}")
        return {"error": f"Timeout Error: {err_timeout}"}, 504
        
    except requests.exceptions.ConnectionError as err_conn:
        logging.error(f"Error Connecting: Não foi possível conectar. {err_conn}")
        return {"error": f"Error Connecting: {err_conn}"}, 503
        
    except requests.exceptions.HTTPError as err_http:
        logging.error(f"HTTP Error: {err_http}. O cookie pode ter expirado.")
        return {"error": f"HTTP Error: {err_http}. Verifique se o cookie expirou."}, response.status_code
        
    except requests.exceptions.RequestException as err:
        logging.error(f"An unexpected error occurred: {err}")
        return {"error": f"An unexpected error occurred: {err}"}, 500
        
    except requests.exceptions.JSONDecodeError:
        logging.error("Failed to decode JSON from API response. A API pode ter retornado um desafio de bot em vez de JSON.")
        return {"error": "Failed to decode JSON from API response"}, 500

get_api_data()