import re
import psycopg2

file = open("amazon-meta.txt", "r", encoding="latin-1")
EOF = False

pattern_info = lambda string: re.match(r'\s*([^:]+):\s*(.*)', string)
extract_asins = lambda string: re.findall(r'\b(\w{10})\b', string)
extract_total_reviews = lambda string: re.search(r'\s*total:\s*(\d+)', string).group(1)

try:
    conn = psycopg2.connect(
    host="localhost",
    database="snapamazon",
    user="postgres",
    password="postgres")
    cur = conn.cursor()

except (Exception, psycopg2.DatabaseError) as error:
    print(error)
    conn.close()

def sql_execute(command, params):
    try:
        cur.execute(command, params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def sql_commit():
    try:
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

"""
 Para visualizar as tabelas, entre no terminal nesse caminho e "sqlite3 snapamazon.db"
 Dentro do terminal interativo do sqlite ".tables"
"""

commands = (
        """
        CREATE TABLE IF NOT EXISTS Products (
        ASIN VARCHAR(20) PRIMARY KEY,
        title VARCHAR(452),
        group_type VARCHAR(20),
        salesrank INT
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS ProductCategories (
        ProductASIN VARCHAR(20),
        category VARCHAR(255),
        PRIMARY KEY (ProductASIN, category),
        FOREIGN KEY (ProductASIN) REFERENCES Products(ASIN)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS SimilarProducts (
        ProductASIN VARCHAR(20),
        similarASIN VARCHAR(20),
        PRIMARY KEY (ProductASIN, similarASIN),
        FOREIGN KEY (ProductASIN) REFERENCES Products(ASIN)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Reviews (
        Id SERIAL PRIMARY KEY,
        ProductASIN VARCHAR(20) REFERENCES Products(ASIN),
        review_date DATE,
        customer_id VARCHAR(20),
        rating INT,
        votes INT,
        helpful INT
        )
        """)


for command in commands:
    try:
        cur.execute(command)
        conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)


while(EOF != True):
    line = file.readline()
    if(line == ""): break
    
    match = pattern_info(line)
    
    if(match):
        if(match.group(1) == "ASIN"):
            sql_commit()
            asin = match.group(2)
            sql_execute("""
            INSERT INTO Products(ASIN) VALUES (%s)
            """, [asin])

        elif(match.group(1) == "title"):
            # apenas adiciona na base de dados, visto que title aparece apenas na Tabela Product
            sql_execute("""
            UPDATE Products SET title = %s WHERE ASIN = %s
            """, [match.group(2), asin])

        elif(match.group(1) == "group"):
            # apenas adiciona na base de dados, visto que aparece apenas na Tabela Product
            sql_execute("""
            UPDATE Products SET group_type = %s WHERE ASIN = %s
            """, [match.group(2), asin])
        
        elif(match.group(1) == "salesrank"):
            # apenas adiciona na base de dados, visto que aparece apenas na Tabela Product
            sql_execute("""
            UPDATE Products SET salesrank = %s WHERE ASIN = %s
            """, [match.group(2), asin])

        elif(match.group(1) == "similar"):
            similarProducts = extract_asins(match.group(2))
            for similarProduct in similarProducts:
                sql_execute("""
                INSERT INTO SimilarProducts(ProductASIN, similarASIN) VALUES (%s, %s) 
                """, [asin, similarProduct])

        elif(match.group(1) == "categories"):
            for x in range(int(match.group(2))):
                category = file.readline().replace("\n", "")
                sql_execute("""
                INSERT INTO ProductCategories(ProductASIN, category) VALUES (%s, %s) 
                """, [asin, category])

        elif(match.group(1) == "reviews"):
            total_reviews = int(extract_total_reviews(match.group(2)))

            line = file.readline()
            while(line != '\n'): # pega até a quebra de linha do proximo bloco de info
                partes = line.split()
                data = partes[0]
                cutomer = partes[2]
                rating = int(partes[4])
                votes = int(partes[6])
                helpful = int(partes[8])

                sql_execute("""
                INSERT INTO Reviews(ProductASIN, review_date, customer_id, rating, votes, helpful)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, [asin, data, cutomer, rating, votes, helpful])

                line = file.readline()
            
sql_commit()
conn.close()