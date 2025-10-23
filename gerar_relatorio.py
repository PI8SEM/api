import json
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import HorizontalBarChart


def criar_grafico_horizontal(titulo, categorias, valores, cor):
    """Cria um gráfico de barras horizontais com título"""
    d = Drawing(400, 200)
    grafico = HorizontalBarChart()
    grafico.x = 50
    grafico.y = 40
    grafico.height = 125
    grafico.width = 300
    grafico.data = [valores]
    grafico.categoryAxis.categoryNames = categorias
    grafico.valueAxis.valueMin = 0
    grafico.valueAxis.valueMax = max(valores) * 1.2  # margem de 20%
    grafico.bars[0].fillColor = cor
    grafico.barWidth = 10
    grafico.groupSpacing = 15

    d.add(grafico)
    d.add(String(130, 180, titulo, fontSize=12, fillColor=colors.black))
    return d


def gerar_pdf(dados):
    """Gera o relatório PDF com base nos dados do dicionário"""
    caminho_pdf = "relatorio_telemetria.pdf"
    pdf = SimpleDocTemplate(caminho_pdf, pagesize=A4)
    styles = getSampleStyleSheet()
    conteudo = []

    # Cabeçalho
    conteudo.append(Paragraph("Relatório de Telemetria Elétrica", styles["Title"]))
    conteudo.append(Spacer(1, 12))
    conteudo.append(Paragraph(f"Data de inserção: {dados.get('data_inc', 'N/A')}", styles["Normal"]))
    conteudo.append(Spacer(1, 20))

    # Monta tabela e gráficos no conteúdo
    criar_tabela_dinamica(dados, conteudo)

    # Gera o PDF
    pdf.build(conteudo)
    print("✅ Relatório PDF gerado: relatorio_telemetria.pdf")

    return caminho_pdf

def criar_tabela_dinamica(dados, conteudo):    
    tabela_dados = [["Parâmetro", "Fase 1", "Fase 2", "Fase 3"]]
    parametros = {}

    # Agrupa chaves que pertencem ao mesmo parâmetro
    for chave, valor in dados.items():
        if "_" in chave:
            base = chave.rsplit("_", 1)[0]
            if base not in parametros:
                parametros[base] = {}
            if chave.endswith("_1"):
                parametros[base]["1"] = valor
            elif chave.endswith("_2"):
                parametros[base]["2"] = valor
            elif chave.endswith("_3"):
                parametros[base]["3"] = valor
            
    # Cria as linhas da tabela
    for nome, fases in parametros.items():
        tabela_dados.append([
            nome.replace("_", " ").capitalize(),
            fases["1"],
            fases["2"],
            fases["3"]            
        ])

    
    tabela = Table(tabela_dados, hAlign="LEFT")
    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold")
    ]))
    conteudo.append(tabela)
    conteudo.append(Spacer(1, 30))

    # ----------------------------
    # Gráficos horizontais
    # ----------------------------
    grafico_tensao = criar_grafico_horizontal(
        "Tensão por Fase (V)",
        ["Fase 1", "Fase 2", "Fase 3"],
        [dados["tensao_1"], dados["tensao_2"], dados["tensao_3"]],
        colors.blue
    )
    conteudo.append(grafico_tensao)
    conteudo.append(Spacer(1, 20))

    grafico_corrente = criar_grafico_horizontal(
        "Corrente por Fase (A)",
        ["Fase 1", "Fase 2", "Fase 3"],
        [dados["corrente_1"], dados["corrente_2"], dados["corrente_3"]],
        colors.green
    )
    conteudo.append(grafico_corrente)
    conteudo.append(Spacer(1, 20))


    # fim da função — geração do PDF é feita por gerar_pdf