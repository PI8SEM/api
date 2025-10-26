import json
import pandas as pd
import io
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER
from reportlab.lib.units import inch, mm, cm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, 
                              Table, TableStyle, Image, PageBreak)
from reportlab.lib.utils import simpleSplit
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

# --- Registro de Fontes Times ---
try:
    pdfmetrics.registerFont(TTFont('Times-Roman', 'times.ttf'))
    pdfmetrics.registerFont(TTFont('Times-Bold', 'timesbd.ttf'))
except Exception as e:
    print(f"Aviso: Fontes Times (times.ttf) não encontradas ({e}). A numeração de página usará Helvetica.")
    pass

class ReportGeneratorABNT:
    """
    Classe para gerar relatórios técnicos PDF seguindo as normas ABNT.
    """
    def __init__(self, file_name):
        """
        Inicializa o gerador de relatório.
        """
        self.file_name = file_name
        self.story = []
        
        # --- Contadores Automáticos ---
        self.cont_secao = 0
        self.cont_subsecao = 0
        self.cont_figura = 0
        self.cont_tabela = 0
        
        # --- CORREÇÃO (1/4): Adiciona variáveis para a capa ---
        self.capa_cidade = ""
        self.capa_ano = ""
        
        # --- Configuração de Margens ABNT ---
        self.rightMargin = 2*cm
        self.leftMargin = 3*cm
        self.topMargin = 3*cm
        self.bottomMargin = 2*cm
        
        # --- Configuração de Estilos ABNT ---
        self.styles = getSampleStyleSheet()
        self._definir_estilos()

    def _definir_estilos(self):
        """Método privado para criar todos os estilos ABNT."""
        
        # 0. Corpo de Texto (Base)
        self.styles.add(ParagraphStyle(
            name='BodyTextABNT', parent=self.styles['BodyText'],
            fontName='Times-Roman', fontSize=12, leading=18,
            alignment=TA_JUSTIFY, firstLineIndent=1.25*cm, spaceAfter=0
        ))

        # 1. Estilos da Capa (Centralizados)
        self.styles.add(ParagraphStyle(
            name='CapaInstituicao', parent=self.styles['Normal'],
            fontName='Times-Roman', fontSize=12, alignment=TA_CENTER,
            spaceAfter=1*cm
        ))
        self.styles.add(ParagraphStyle(
            name='CapaAutor', parent=self.styles['CapaInstituicao'],
            spaceAfter=3*cm
        ))
        self.styles.add(ParagraphStyle(
            name='CapaTitulo', parent=self.styles['Normal'],
            fontName='Times-Bold', fontSize=16, alignment=TA_CENTER,
            spaceBefore=3*cm, spaceAfter=3*cm
        ))
        
        # --- CORREÇÃO (2/4): Estilos 'CapaLocal' e 'CapaAno' removidos ---
        # (Não são mais necessários, pois serão desenhados no canvas)
        
        # 2. Estilos de Seção (Títulos)
        self.styles.add(ParagraphStyle(
            name='SecaoABNT', parent=self.styles['Normal'],
            fontName='Times-Bold', fontSize=12, leading=18,
            alignment=TA_LEFT, spaceAfter=6, spaceBefore=12
        ))
        self.styles.add(ParagraphStyle(
            name='SubsecaoABNT', parent=self.styles['SecaoABNT'],
            fontName='Times-Roman',
        ))

        # 3. Estilos de Legenda (Figuras e Tabelas)
        self.styles.add(ParagraphStyle(
            name='LegendaABNT', parent=self.styles['Normal'],
            fontName='Times-Roman', fontSize=10, leading=12,
            alignment=TA_CENTER, spaceAfter=3*mm
        ))

        # 4. Estilo de Bloco de Código
        self.styles.add(ParagraphStyle(
            name='CodigoFonte', parent=self.styles['Normal'],
            fontName='Courier', fontSize=10, leading=12,
            alignment=TA_LEFT, leftIndent=1.25*cm,
            backColor=colors.HexColor("#F0F0F0"),
            borderPadding=5, borderColor=colors.HexColor("#DDDDDD"),
            borderWidth=1, spaceBefore=5*mm, spaceAfter=5*mm
        ))
        
        # 5. Estilos de Lista
        self.styles.add(ParagraphStyle(
            name='ListaBase', parent=self.styles['BodyText'],
            fontName='Times-Roman', fontSize=12, leading=18,
            firstLineIndent=0, leftIndent=1.75*cm, 
            bulletIndent=1.25*cm, spaceAfter=3,
            alignment=TA_LEFT 
        ))
        self.styles.add(ParagraphStyle(
            name='ListaBullet', parent=self.styles['ListaBase'],
            bulletFontName='Symbol',
        ))
        self.styles.add(ParagraphStyle(
            name='ListaNumerada', parent=self.styles['ListaBase'],
        ))

    # --- Métodos de Geração (Públicos) ---

    def gerar_pdf(self):
        """
        Gera o relatório PDF final com o conteúdo, capa e numeração.
        """
        try:
            pdf = SimpleDocTemplate(
                f"./reports/{self.file_name}.pdf",
                pagesize=A4,
                rightMargin=self.rightMargin,
                leftMargin=self.leftMargin,
                topMargin=self.topMargin,
                bottomMargin=self.bottomMargin
            )
            # Adiciona a numeração de página
            pdf.build(
                self.story,
                onFirstPage=self._placeholder_capa,  # <-- CORREÇÃO: Usará esta função
                onLaterPages=self._add_numeracao
            )
            print(f"Relatório '{self.file_name}.pdf' gerado com sucesso!")
        except Exception as e:
            print(f"Erro ao gerar PDF: {e}")

    # --- Métodos de Callback (Privados) ---

    def _placeholder_capa(self, canvas, doc):
        """CORREÇÃO (4/4): Desenha os elementos do rodapé da capa (cidade e ano)."""
        canvas.saveState()
        
        # Define a fonte e tamanho
        font_size = 12
        try:
            canvas.setFont('Times-Roman', font_size)
        except:
            canvas.setFont('Helvetica', font_size) # Fallback

        # Posição (Centralizado, na margem inferior)
        x_central = A4[0] / 2  # Centro horizontal da página
        
        # Posição Y (a partir da margem inferior de 2cm)
        y_ano = self.bottomMargin # 2cm do fundo
        y_cidade = y_ano + (font_size * 1.2) # 1.2 de "entrelinha"
        
        # Desenha a cidade (centralizada)
        if self.capa_cidade:
            canvas.drawCentredString(x_central, y_cidade, self.capa_cidade)
        
        # Desenha o ano (centralizado)
        if self.capa_ano:
            canvas.drawCentredString(x_central, y_ano, self.capa_ano)
        
        canvas.restoreState()


    def _add_numeracao(self, canvas, doc):
        """Adiciona o número da página no canto superior direito (ABNT)."""
        canvas.saveState()
        
        page_num = canvas.getPageNumber()
        if page_num > 1:
            x = A4[0] - self.rightMargin
            y = A4[1] - self.topMargin + 1*cm 
            
            try:
                canvas.setFont('Times-Roman', 10)
            except:
                canvas.setFont('Helvetica', 10)
            
            canvas.drawRightString(x, y, str(page_num))
        
        canvas.restoreState()


    # --- Novos Métodos de Conteúdo ---

    def add_capa(self, instituicao, autor, titulo, cidade, ano):
        """
        CORREÇÃO (3/4): Salva cidade/ano e adiciona o resto ao story.
        """
        # Salva cidade e ano para serem desenhados pelo _placeholder_capa
        self.capa_cidade = cidade
        self.capa_ano = ano
        
        # Adiciona elementos centrais ao story
        self.story.append(Paragraph(instituicao, self.styles['CapaInstituicao']))
        self.story.append(Paragraph(autor, self.styles['CapaAutor']))
        self.story.append(Paragraph(titulo.upper(), self.styles['CapaTitulo']))
        
        # Cidade e Ano foram removidos daqui
        
        self.nova_pagina() # Força que o conteúdo comece na pág 2

    def nova_pagina(self):
        """Força uma quebra de página."""
        self.story.append(PageBreak())

    def secao(self, texto):
        """Adiciona um Título de Seção principal (ex: 1. TÍTULO)."""
        self.cont_secao += 1
        self.cont_subsecao = 0 
        titulo_formatado = f"{self.cont_secao}. {texto.upper()}"
        self.story.append(Paragraph(titulo_formatado, self.styles['SecaoABNT']))

    def subsecao(self, texto):
        """Adiciona um Título de Subseção (ex: 1.1 Título)."""
        self.cont_subsecao += 1
        titulo_formatado = f"{self.cont_secao}.{self.cont_subsecao} {texto}"
        self.story.append(Paragraph(titulo_formatado, self.styles['SubsecaoABNT']))

    def legenda_figura(self, texto):
        """Adiciona uma legenda de Figura (ex: Figura 1 - Texto). CHAME *DEPOIS* da imagem."""
        self.cont_figura += 1
        legenda = f"Figura {self.cont_figura} - {texto}"
        self.story.append(Paragraph(legenda, self.styles['LegendaABNT']))

    def legenda_tabela(self, texto):
        """Adiciona uma legenda de Tabela (ex: Tabela 1 - Texto). CHAME *ANTES* da tabela."""
        self.cont_tabela += 1
        legenda = f"Tabela {self.cont_tabela} - {texto}"
        self.story.append(Paragraph(legenda, self.styles['LegendaABNT']))

    def bloco_codigo(self, texto_codigo):
        """Adiciona um bloco de código formatado."""
        texto_formatado = texto_codigo.replace('\n', '<br/>').replace(' ', '&nbsp;')
        self.story.append(Paragraph(texto_formatado, self.styles['CodigoFonte']))

    def lista_numerada(self, itens):
        """Adiciona uma lista numerada a partir de uma lista Python."""
        idx = 0
        for item in itens:
            idx += 1
            p = Paragraph(
                f"{item}", 
                self.styles['ListaNumerada'], 
                bulletText=f'{idx}.'
            )
            self.story.append(p)
        self.story.append(Spacer(1, 3*mm))

    def lista_bullet(self, itens):
        """Adiciona uma lista com marcadores (bullet) a partir de uma lista Python."""
        for item in itens:
            p = Paragraph(
                f"{item}", 
                self.styles['ListaBullet'], 
                bulletText='•'
            )
            self.story.append(p)
        self.story.append(Spacer(1, 3*mm))

    # --- Métodos Originais (Adaptados para a Classe) ---

    def paragrafo(self, texto):
        """Adiciona um Parágrafo de corpo de texto (ABNT) ao relatório."""
        self.story.append(Paragraph(texto, self.styles["BodyTextABNT"]))

    def imagem(self, caminho_imagem, largura_mm):
        """Adiciona uma Imagem (Centralizada) ao relatório."""
        largura_max_mm = (A4[0] / mm) - (self.leftMargin / mm) - (self.rightMargin / mm)
        if largura_mm > largura_max_mm:
            largura_mm = largura_max_mm

        img = Image(caminho_imagem, width=largura_mm*mm, height=None)
        img.drawHeight = largura_mm * mm * (img.imageHeight / img.imageWidth) 
        img.hAlign = 'CENTER'
        self.story.append(img)

    def espaco(self, tamanho_mm):
        """Adiciona um Espaçador vertical ao relatório."""
        self.story.append(Spacer(1, tamanho_mm*mm))

    def tabela_pandas(self, df, colWidths_mm=None):
        """Adiciona uma Tabela a partir de um DataFrame (Fontes ABNT)."""
        dados_lista = [df.columns.values.tolist()] + df.values.tolist()
        
        if colWidths_mm:
            larguras = [w * mm for w in colWidths_mm]
        else:
            largura_total_util = (A4[0] - self.leftMargin - self.rightMargin) 
            num_cols = len(df.columns)
            larguras = [largura_total_util / num_cols] * num_cols

        tbl = Table(dados_lista, colWidths=larguras)
        
        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#4472C4")), 
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, 0), 'Times-Bold'), 
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('FONTNAME', (0, 1), (-1, -1), 'Times-Roman'), 
            ('FONTSIZE', (0, 1), (-1, -1), 10), 
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'), 
            ('PADDING-LEFT', (0, 1), (0, -1), 5), 
        ])
        
        tbl.setStyle(style)
        tbl.hAlign = 'CENTER' 
        self.story.append(tbl)

    def grafico_pandas(self, df, x_col, y_col, titulo_grafico, 
                       tipo_grafico='bar', largura_mm=160, altura_mm=100):
        """Gera um gráfico (Centralizado) e o adiciona ao relatório."""
        
        try:
            df_plot = df[df[x_col].str.lower() != 'total'].copy()
            df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
            df_plot = df_plot.dropna(subset=[y_col])
        except AttributeError: 
            df_plot = df.copy()
            df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
            df_plot = df_plot.dropna(subset=[y_col])

        if df_plot.empty:
            print(f"Aviso: Não foi possível gerar o gráfico '{titulo_grafico}' (dados vazios).")
            return

        largura_max_mm = (A4[0] / mm) - (self.leftMargin / mm) - (self.rightMargin / mm)
        if largura_mm > largura_max_mm:
            largura_mm = largura_max_mm

        fig, ax = plt.subplots(figsize=(largura_mm / 25.4, altura_mm / 25.4)) 
        try:
            plt.rcParams['font.family'] = 'Times New Roman'
        except:
            print("Aviso: Fonte 'Times New Roman' não encontrada para Matplotlib. Usando padrão.")
            plt.rcParams['font.family'] = 'sans-serif'
            
        plt.rcParams['font.size'] = 10

        df_plot.plot(kind=tipo_grafico, x=x_col, y=y_col, ax=ax, legend=False)
        ax.set_title(titulo_grafico, fontsize=12, fontweight='bold')
        ax.set_xlabel(x_col, fontsize=10)
        ax.set_ylabel(y_col, fontsize=10)
        ax.tick_params(axis='x', rotation=45) 
        plt.tight_layout() 
        
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='PNG', dpi=300)
        img_buffer.seek(0)
        plt.close(fig) 
        
        img = Image(img_buffer, width=largura_mm*mm, height=altura_mm*mm)
        img.hAlign = 'CENTER'
        self.story.append(img)


# --- Funções de Exemplo (Externas à classe) ---
# (Deixadas inalteradas, pois o 'if __name__ == "__main__":' 
# já usa a classe corrigida)

def carregar_dados_json(caminho_json):
    """Carrega dados de um arquivo JSON"""
    try:
        with open(caminho_json, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        return dados
    except Exception as e:
        print(f"Erro ao carregar JSON: {e}")
        return None

def criar_json_exemplo():
    """Cria um arquivo 'dados.json' para o script poder rodar"""
    dados_ex = {
        "titulo_relatorio": "Relatório de Demonstração Completa",
        "introducao": "Este é o primeiro parágrafo do relatório...",
        "dados_tabela_pandas": [
            {"Produto": "Produto A", "Unidades Vendidas": 150, "Receita (R$)": "R$ 15.000,00"},
            {"Produto": "Produto B", "Unidades Vendidas": 220, "Receita (R$)": "R$ 22.000,00"},
            {"Produto": "Produto C", "Unidades Vendidas": 180, "Receita (R$)": "R$ 18.000,00"}
        ],
        "codigo_exemplo": "def carregar_dados_json(caminho_json):\n    ..."
    }
    with open("dados.json", "w", encoding='utf-8') as f:
        json.dump(dados_ex, f, indent=4, ensure_ascii=False) 

def criar_imagem_exemplo():
    """Cria um arquivo 'logo_exemplo.png' para o script poder rodar"""
    try:
        c = canvas.Canvas("logo_exemplo.png", pagesize=(200, 200))
        c.setFillColorRGB(0.1, 0.5, 0.9)
        c.rect(0, 0, 200, 200, fill=1, stroke=0)
        c.setFillColorRGB(1, 1, 1)
        c.setFont("Helvetica-Bold", 40)
        c.drawCentredString(100, 100, "LOGO")
        c.save()
    except Exception as e:
        print(f"Erro ao criar imagem de exemplo: {e}")

def orquestrar_relatorio(json_data, nome_arquivo_saida):
    """
    Consome um dicionário JSON e chama os métodos da classe ReportGeneratorABNT
    para construir o PDF.
    """
    
    # 1. Instancia o relatório
    report = ReportGeneratorABNT(nome_arquivo_saida)
    
    # 2. Itera sobre a lista de elementos do JSON
    elementos = json_data.get('elementos', [])
    
    for elemento in elementos:
        # Pega o nome do método e seus argumentos
        # (Cada 'elemento' é um dict com uma única chave)
        try:
            metodo, args = list(elemento.items())[0]
        except IndexError:
            print("Aviso: Elemento JSON vazio encontrado. Ignorando.")
            continue
            
        # 3. Chama o método correspondente na instância do relatório
        try:
            if metodo == "add_capa":
                # args é um dict, desempacota com **
                report.add_capa(**args)
            elif metodo == "secao":
                report.secao(args)
            elif metodo == "subsecao":
                report.subsecao(args)
            elif metodo == "paragrafo":
                report.paragrafo(args)
            elif metodo == "lista_numerada":
                report.lista_numerada(args)
            elif metodo == "lista_bullet":
                report.lista_bullet(args)
            elif metodo == "legenda_tabela":
                report.legenda_tabela(args)
            elif metodo == "tabela_pandas":
                # Transformação: JSON (lista de dicts) -> DataFrame
                df = pd.DataFrame(args['data'])
                colWidths = args.get('colWidths_mm', None)
                report.tabela_pandas(df, colWidths_mm=colWidths)
            elif metodo == "legenda_figura":
                report.legenda_figura(args)
            elif metodo == "imagem":
                # args é um dict {'caminho_imagem': '...', 'largura_mm': ...}
                report.imagem(**args)
            elif metodo == "grafico_pandas":
                # Transformação: JSON -> DataFrame + args
                df = pd.DataFrame(args['data'])
                report.grafico_pandas(
                    df, 
                    x_col=args['x_col'], 
                    y_col=args['y_col'], 
                    titulo_grafico=args['titulo_grafico'],
                    tipo_grafico=args.get('tipo_grafico', 'bar'),
                    largura_mm=args.get('largura_mm', 160),
                    altura_mm=args.get('altura_mm', 100)
                )
            elif metodo == "bloco_codigo":
                report.bloco_codigo(args)
            elif metodo == "espaco":
                report.espaco(args)
            elif metodo == "nova_pagina":
                report.nova_pagina()
            else:
                print(f"Aviso: Método desconhecido '{metodo}' no JSON. Ignorando.")
                
        except Exception as e:
            print(f"Erro ao processar o elemento '{metodo}': {e}")
            
    # 4. Gera o PDF final
    report.gerar_pdf()

def carregar_json_arquivo(caminho_json):
    """Carrega dados de um arquivo JSON"""
    try:
        with open(caminho_json, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        return dados
    except FileNotFoundError:
        print(f"Erro: Arquivo JSON '{caminho_json}' não encontrado.")
    except json.JSONDecodeError:
        print(f"Erro: Falha ao decodificar o JSON em '{caminho_json}'.")
    except Exception as e:
        print(f"Erro desconhecido ao carregar JSON: {e}")
    return None

if __name__ == "__main__":
    arquivo_json_entrada = "dados.json"
    arquivo_pdf_saida = "Relatorio_Orquestrado"
    
    # 3. Carrega o JSON
    print(f"Carregando dados de '{arquivo_json_entrada}'...")
    dados_relatorio = carregar_json_arquivo(arquivo_json_entrada)
    
    # 4. Executa o orquestrador (se o JSON foi carregado)
    if dados_relatorio:
        print(f"Iniciando orquestração para '{arquivo_pdf_saida}.pdf'...")
        orquestrar_relatorio(dados_relatorio, arquivo_pdf_saida)
    else:
        print("Falha ao carregar o JSON. Geração do relatório cancelada.")