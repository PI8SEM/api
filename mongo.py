from pymongo import MongoClient
import gridfs
from urllib.parse import quote_plus

# Ideal seria um secret key em vez de exposto
password = quote_plus('Tatubola1@')
uri = "mongodb+srv://projeto_8:" + password + "@cluster0.vryuiju.mongodb.net/?appName=Cluster0"
client = MongoClient(uri)

# Escolha o banco de dados onde os arquivos serão armazenados
db = client['Relatorio']

# Inicializa o GridFS
fs = gridfs.GridFS(db)

pdf_path = '/content/ATIVIDADE DE AVALIAÇÃO_P1_3.pdf'

# Abrir o arquivo PDF e armazená-lo no MongoDB
with open(pdf_path, 'rb') as pdf_file:
    pdf_id = fs.put(pdf_file, filename="arquivo3.pdf")

print(f"Arquivo PDF carregado com sucesso. ID do arquivo no MongoDB: {pdf_id}")


# Forma mais pratico de achar o arquivo, utiliza o nome do arquivo em vez de id
def retrieve_file(file_name, output_path):
    """Retrieves a file from MongoDB GridFS and saves it locally."""
    file_data = fs.find_one({"filename": file_name})
    if file_data:
        with open(output_path, "wb") as output_file:
            output_file.write(file_data.read())
        print(f"File {file_name} retrieved successfully.")
    else:
        print(f"File {file_name} not found.")

# Example usage
retrieve_file("example.pdf", "retrieved_example.pdf")        

#Metodo usado buscando o ID, menos pratico
pdf = fs.get(pdf_id)

# Salvando o arquivo recuperado no sistema local
with open('recuperado_arquivo.pdf', 'wb') as f:
    f.write(pdf.read())

print("Arquivo PDF recuperado com sucesso.")
