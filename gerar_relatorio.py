import json
import pandas as pd
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

    # Gera o PDF
    pdf.build(conteudo)
    print("✅ Relatório PDF gerado: relatorio_telemetria.pdf")

    return caminho_pdf

    
    

    # fim da função — geração do PDF é feita por gerar_pdf