from flask import Flask, render_template, request, send_file
import sqlite3
import pandas as pd
import os
from io import BytesIO
import webbrowser
from difflib import get_close_matches
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
    "Mercado Livre": ["Data da venda", "SKU", "Variação", "Total (BRL)", "Unidades"],
    "TikTok": [
        'Order paid time.',
        'Seller sku input by the seller in the product system.',
        'Platform SKU variation',
        'It equals SKU Subtotal Before Discount - SKU Platform Discount - SKU Seller Discount.',
        'SKU sold quantity in the order.'
    ],
    "Shein": ["SKU do vendedor", "Data e hora de criação do pedido", "Variação", "Receita estimada de mercadorias", "Quantidade"]
}
header_map = {
    "Shopee": 0,
    "Mercado Livre": 5,
    "TikTok": 1,
    # Ajuste só para Shein
    "Shein": 1
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

        # Remove vendas anteriores no mesmo intervalo (reseta o período)
        conn = sqlite3.connect("vendas.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM vendas WHERE data BETWEEN ? AND ?", (data_inicio_str, data_fim_str))
        conn.commit()
        conn.close()

        # Lê arquivos Excel da pasta e insere no banco
        pasta = "./"
        arquivos = [f for f in os.listdir(pasta) if f.endswith(".xlsx")]
        for arquivo in arquivos:
            plataforma = detecta_plataforma(arquivo)
            if plataforma == "Outro":
                continue

            df = pd.read_excel(arquivo, header=header_map.get(plataforma, 0))
            print(f"{plataforma} - df.columns: {df.columns.tolist()}")
            colunas = colunas_por_plataforma[plataforma]
            colunas_existentes = [c for c in colunas if c in df.columns]
            print(f"{plataforma} - colunas_existentes: {colunas_existentes}")

            # Filtro para Shopee: ignora cancelados
            if plataforma == "Shopee" and "Status do pedido" in df.columns:
                df = df[df["Status do pedido"].str.lower() != "cancelado"]

            # Filtro para TikTok: ignora cancelados
            if plataforma == "TikTok" and "Order Status" in df.columns:
                df = df[~df["Order Status"].str.lower().str.contains("cancel")]

            # Filtro para Mercado Livre: ignora cancelados
            if plataforma == "Mercado Livre" and "Estado" in df.columns:
                df = df[df["Estado"].str.lower() != "cancelada pelo comprador"]

            # Filtro para Shein: ignora cancelados
            if plataforma == "Shein" and "Status do pedido" in df.columns:
                df = df[df["Status do pedido"].str.lower() != "reembolsado por cliente"]

            # Para Shein, aceita 4 colunas e preenche quantidade=1
            if plataforma == "Shein" and len(colunas_existentes) == 4:
                df = df[colunas_existentes]
                df = df.rename(columns={
                    colunas_existentes[0]: "sku",
                    colunas_existentes[1]: "data",
                    colunas_existentes[2]: "variacao",
                    colunas_existentes[3]: "valor_liquido"
                })
                df["quantidade"] = 1
                # Parser customizado para datas da Shein
                meses = {
                    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
                    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
                    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
                }
                def parse_data_shein(s):
                    import re
                    match = re.match(r"(\d{2}) (\w+) (\d{4}) (\d{2}):(\d{2})", str(s))
                    if match:
                        dia, mes, ano, hora, minuto = match.groups()
                        mes_num = meses.get(mes.lower(), '01')
                        return pd.Timestamp(f"{ano}-{mes_num}-{dia} {hora}:{minuto}")
                    return pd.NaT
                df["data"] = df["data"].apply(parse_data_shein)
            else:
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

            print(f"{plataforma} - Colunas: {df.columns.tolist()}")
            print(f"{plataforma} - Primeiras linhas:\n{df.head(3)}")

            # Conversão especial para valor do TikTok
            if plataforma == "TikTok":
                df["valor_liquido"] = df["valor_liquido"].astype(str).str.replace("BRL", "", regex=False).str.replace(",", ".", regex=False).str.strip()
                df["valor_liquido"] = pd.to_numeric(df["valor_liquido"], errors="coerce").fillna(0)

            # Parser customizado para datas do Mercado Livre
            def parse_data_ml(s):
                import re
                meses = {
                    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
                    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
                    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
                }
                try:
                    match = re.match(r"(\d{1,2}) de (\w+) de (\d{4}) (\d{2}):(\d{2}) hs\.", str(s))
                    if match:
                        dia, mes, ano, hora, minuto = match.groups()
                        mes_num = meses.get(mes.lower(), '01')
                        return pd.Timestamp(f"{ano}-{mes_num}-{dia} {hora}:{minuto}")
                except Exception:
                    pass
                return pd.NaT

            if plataforma == "Mercado Livre":
                df["data"] = df["data"].apply(parse_data_ml)
            else:
                df["data"] = pd.to_datetime(df["data"], errors="coerce")

            print(f"{plataforma} - Datas antes do filtro: {df['data'].head(5).tolist()}")

            df["data_str"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df = df[(df["data"] >= data_inicio_dt) & (df["data"] <= data_fim_dt)]

            print(f"{plataforma} - Linhas após filtro de datas: {len(df)}")

            if plataforma == "Shein":
                df["quantidade"] = 1  #quantidade de 1 por linha.
            print(f"{plataforma} - Linhas lidas: {len(df)} - Colunas: {df.columns.tolist()}")
            print(df.head(2))
            
            df["data"] = pd.to_datetime(df["data"], errors="coerce")
            df["data_str"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")

            
            df = df[(df["data"] >= data_inicio_dt) & (df["data"] <= data_fim_dt)]
           
            if sku_especifico:
                df = df[df["sku"] == sku_especifico]

            # Insere no banco
            conn = sqlite3.connect("vendas.db")
            cursor = conn.cursor()
            for _, row in df.iterrows():
                # evita duplicadas: verifica se já existe uma venda com os mesmos campos
                cursor.execute("""
                    SELECT 1 FROM vendas WHERE sku = ? AND plataforma = ? AND quantidade = ? AND valor_liquido = ? AND data = ?
                """, (
                    row["sku"],
                    plataforma,
                    row["quantidade"],
                    row["valor_liquido"],
                    row["data_str"]
                ))
                if cursor.fetchone() is None:
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