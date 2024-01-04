import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="snapamazon",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

#a
def listar_comentarios_uteis_avaliacao(asin):
    query_maior_avaliacao = f"""
    SELECT *
    FROM Reviews
    WHERE ProductASIN = '{asin}'
    ORDER BY helpful DESC, rating DESC
    LIMIT 5;
    """

    query_menor_avaliacao = f"""
    SELECT *
    FROM Reviews
    WHERE ProductASIN = '{asin}'
    ORDER BY helpful DESC, rating ASC
    LIMIT 5;
    """

    cur.execute(query_maior_avaliacao)
    print("Comentários mais úteis e com maior avaliação:")
    rows = cur.fetchall()
    for row in rows:
        print(row)

    cur.execute(query_menor_avaliacao)
    rows = cur.fetchall()
    print("\nComentários mais úteis e com menor avaliação:")
    for row in rows:
        print(row)

#b
def listar_similares_maiores_vendas(asin):
    query_similares_vendas = f"""
    -- Subconsulta para obter o salesrank do produto de referência
    WITH ReferenceProduct AS (
    SELECT ASIN, salesrank
    FROM Products
    WHERE ASIN = '{asin}'
    )
    -- Consulta principal para obter produtos similares com maiores vendas
    SELECT P.ASIN, P.title, P.salesrank
    FROM SimilarProducts SP
    JOIN Products P ON SP.similarASIN = P.ASIN
    JOIN ReferenceProduct RP ON SP.ProductASIN = RP.ASIN
    WHERE P.salesrank < RP.salesrank
    ORDER BY P.salesrank;
    """

    cur.execute(query_similares_vendas)
    rows = cur.fetchall()
    for row in rows:
        print(row)

#c
def mostrar_evolucao_medias_avaliacao(asin):
    query_evolucao_avaliacao = f"""
    SELECT review_date, AVG(rating) AS avg_rating
    FROM Reviews
    WHERE ProductASIN = '{asin}'
    GROUP BY review_date
    ORDER BY review_date;
    """

    cur.execute(query_evolucao_avaliacao)
    rows = cur.fetchall()
    for row in rows:
        print(row)

#d
def listar_lideres_venda():
    query_lideres_venda = """
    WITH RankedProducts AS (
    SELECT ASIN, title, group_type, salesrank, ROW_NUMBER() OVER (PARTITION BY group_type ORDER BY salesrank) AS rank
    FROM Products
    WHERE salesrank > 0
    )
    SELECT ASIN, title, group_type, salesrank
    FROM RankedProducts
    WHERE rank <= 10
    ORDER BY group_type, rank;
    """

    cur.execute(query_lideres_venda)
    rows = cur.fetchall()
    for row in rows:
        print(row)

#e
def listar_top10_avaliacoes_uteis():
    query_top10_avaliacoes_uteis = """
    SELECT r.ProductASIN AS ASIN, p.title, AVG(r.helpful) AS avg_helpful
    FROM Reviews r
    JOIN Products p ON r.ProductASIN = p.ASIN
    GROUP BY r.ProductASIN, p.title
    ORDER BY avg_helpful DESC
    LIMIT 10;
    """

    cur.execute(query_top10_avaliacoes_uteis)
    rows = cur.fetchall()
    for row in rows:
        print(row)

#f
def listar_top5_categorias_avaliacoes_uteis():
    query_top5_categorias_avaliacoes_uteis = """
    SELECT pc.category, AVG(r.helpful) AS avg_helpful
    FROM ProductCategories pc
    JOIN Reviews r ON pc.ProductASIN = r.ProductASIN
    GROUP BY pc.category
    ORDER BY avg_helpful DESC
    LIMIT 5;
    """

    cur.execute(query_top5_categorias_avaliacoes_uteis)
    rows = cur.fetchall()
    for row in rows:
        print(row)

#g
def listar_top10_clientes_comentarios():
    query_top10_clientes_comentarios = """
    WITH RankedComments AS (
    SELECT P.group_type, R.customer_id, COUNT(*) as total_comments, RANK() OVER (PARTITION BY P.group_type ORDER BY COUNT(*) DESC) as rank
    FROM Reviews R
    JOIN Products P ON R.ProductASIN = P.ASIN
    GROUP BY P.group_type, R.customer_id
    )
    SELECT group_type, customer_id, total_comments
    FROM RankedComments
    WHERE rank <= 10;
    """

    cur.execute(query_top10_clientes_comentarios)
    rows = cur.fetchall()
    for row in rows:
        print(row)




opcoes_texto = """
(a) Dado um produto, listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação
(b) Dado um produto, listar os produtos similares com maiores vendas do que ele
(c) Dado um produto, mostrar a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada
(d) Listar os 10 produtos líderes de venda em cada grupo de produtos
(e) Listar os 10 produtos com a maior média de avaliações úteis positivas por produto
(f) Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto
(g) Listar os 10 clientes que mais fizeram comentários por grupo de produto
"""

print(opcoes_texto)

while True:
    opcao = input("Digite a letra correspondente à ação que deseja executar (ou 's' para sair): ")

    if opcao in ['a', 'b', 'c']:
        asin = input("Digite o ASIN do produto: ")
        if opcao == 'a':
            listar_comentarios_uteis_avaliacao(asin)
        elif opcao == 'b':
            listar_similares_maiores_vendas(asin)
        elif opcao == 'c':
            mostrar_evolucao_medias_avaliacao(asin)
    elif opcao == 'd':
        listar_lideres_venda()
    elif opcao == 'e':
        listar_top10_avaliacoes_uteis()
    elif opcao == 'f':
        listar_top5_categorias_avaliacoes_uteis()
    elif opcao == 'g':
        listar_top10_clientes_comentarios()
    elif opcao == 's':
        print("Saindo do programa...")
        break
    else:
        print("Opção inválida. Por favor, escolha uma letra de 'a' a 'g' ou 's' para sair.")

conn.commit()