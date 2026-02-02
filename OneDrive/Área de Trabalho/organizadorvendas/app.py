from flask import Flask, render_template, request, send_file
import sqlite3
import pandas as pd
import os
from io import BytesIO
import webbrowser

app = Flask(__name__)

# Criar o banco e tabela se não existir

conn = sqlite3.connect("vendas.db")
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT,
        plataforma TEXT,
        quantidade INTEGER,
        valor_liquido REAL,
        data TEXT
    )
""")
conn.commit()
conn.close()

# Colunas esperadas por plataforma

colunas_por_plataforma = {
    "Shopee": ["Hora do pagamento do pedido", "Número de referência SKU", "Nome da variação", "Subtotal do produto", "Quantidade"],
    "Mercado Livre": ["Data do pagamento", "SKU", "Variação", "Valor", "Qtd"],
    "TikTok": ["Data", "SKU", "Variação", "Valor", "Quantidade"],
    "Shein": ["Hora do pagamento do pedido", "SKU", "Variação", "Subtotal", "Número de quantidade"]
}

def detecta_plataforma(nome_arquivo):
    nome = nome_arquivo.lower()
    if "shopee" in nome:
        return "Shopee"
    elif "mercadolivre" in nome:
        return "Mercado Livre"
    elif "tiktok" in nome:
        return "TikTok"
    elif "shein" in nome:
        return "Shein"
    else:
        return "Outro"

@app.route("/", methods=["GET", "POST"])
def index():
    resumo = pd.DataFrame()
    mensagem = ""
    grafico_vendas_dia = []
    grafico_produtos = []

    if request.method == "POST":
       
        data_inicio = request.form.get("data_inicio")
        data_fim = request.form.get("data_fim")
        sku_especifico = request.form.get("sku", "").strip()

    
        data_inicio_dt = pd.to_datetime(data_inicio)
        data_fim_dt = pd.to_datetime(data_fim)
        data_inicio_str = data_inicio_dt.strftime("%Y-%m-%d %H:%M:%S")
        data_fim_str = data_fim_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Lê arquivos Excel da pasta e insere no banco
        pasta = "./"
        arquivos = [f for f in os.listdir(pasta) if f.endswith(".xlsx")]
        for arquivo in arquivos:
            plataforma = detecta_plataforma(arquivo)
            if plataforma == "Outro":
                continue

            df = pd.read_excel(arquivo)
            colunas = colunas_por_plataforma[plataforma]
            colunas_existentes = [c for c in colunas if c in df.columns]
            if len(colunas_existentes) < 5:
                continue  # pula arquivos com colunas insuficientes

            df = df[colunas_existentes]
            df = df.rename(columns={
                colunas_existentes[0]: "data",
                colunas_existentes[1]: "sku",
                colunas_existentes[2]: "variacao",
                colunas_existentes[3]: "valor_liquido",
                colunas_existentes[4]: "quantidade"
            })

            
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
            df["data_str"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")

            
            df = df[(df["data"] >= data_inicio_dt) & (df["data"] <= data_fim_dt)]
           
            if sku_especifico:
                df = df[df["sku"] == sku_especifico]

            # Insere no banco
            conn = sqlite3.connect("vendas.db")
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO vendas (sku, plataforma, quantidade, valor_liquido, data)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    row["sku"],
                    plataforma,
                    row["quantidade"],
                    row["valor_liquido"],
                    row["data_str"]
                ))
            conn.commit()
            conn.close()

        # Gera resumo do banco
        conn = sqlite3.connect("vendas.db")
        cursor = conn.cursor()
        if sku_especifico:
            cursor.execute("""
                SELECT sku, plataforma, SUM(quantidade), SUM(valor_liquido)
                FROM vendas
                WHERE data BETWEEN ? AND ?
                AND sku = ?
                GROUP BY sku, plataforma
            """, (data_inicio_str, data_fim_str, sku_especifico))
        else:
            cursor.execute("""
                SELECT sku, plataforma, SUM(quantidade), SUM(valor_liquido)
                FROM vendas
                WHERE data BETWEEN ? AND ?
                GROUP BY sku, plataforma
            """, (data_inicio_str, data_fim_str))

        resumo_dados = cursor.fetchall()
        conn.close()
        resumo = pd.DataFrame(resumo_dados, columns=["SKU", "Plataforma", "Quantidade", "Valor Total"])

        # Gera gráficos
        conn = sqlite3.connect("vendas.db")
        df = pd.read_sql_query(f"""
            SELECT * FROM vendas
            WHERE data BETWEEN '{data_inicio_str}' AND '{data_fim_str}'
        """, conn)
        conn.close()

        if not df.empty:
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
            df_dia = df.groupby(df["data"].dt.date)["quantidade"].sum().reset_index()
            grafico_vendas_dia = df_dia.to_dict(orient="records")

            df_sku = df.groupby("sku")["quantidade"].sum().reset_index()
            grafico_produtos = df_sku.to_dict(orient="records")

    if resumo.empty:
        mensagem = "Não há vendas nesse período."
    else:
        mensagem = f"{len(resumo)} registros encontrados."

    return render_template(
        "index.html",
        resumo=resumo,
        mensagem=mensagem,
        grafico_vendas_dia=grafico_vendas_dia,
        grafico_produtos=grafico_produtos
    )

@app.route("/download", methods=["POST"])
def download():
    dados_json = request.form["dados_excel"]
    df_resumo = pd.read_json(dados_json)
    output = BytesIO()
    df_resumo.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, download_name="resumo_vendas.xlsx", as_attachment=True)

if __name__ == "__main__":
    webbrowser.open("http://127.0.0.1:5000/")
    app.run(debug=True)