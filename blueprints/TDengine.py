import taos

lines = ["d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,'California.SanFrancisco',2",
         "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,'California.LosAngeles',3",
         "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,'California.LosAngeles',2",
         "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,'California.LosAngeles',3",
         "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,'California.SanFrancisco',3",
         "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,'California.SanFrancisco',2",
         "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,'California.SanFrancisco',2",
         "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,'California.LosAngeles',2"]


def get_connection() -> taos.TaosConnection:
    """
    create connection use firstEp in taos.cfg and use default user and password.
    """
    conn = taos.connect(host="localhost",
                        port=6030,
                        user="root",
                        password="taosdata",
                        database="test")
    print('client info:', conn.client_info)
    print('server info:', conn.server_info)
    return conn


def create_stable(conn: taos.TaosConnection):
    conn.execute("CREATE DATABASE power if not exist")
    conn.execute("USE power")
    conn.execute("CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")



def get_sql():
    global lines
    lines = map(lambda line: line.split(','), lines)  # [['d1001', ...]...]
    lines = sorted(lines, key=lambda ls: ls[0])  # sort by table name
    sql = "INSERT INTO "
    tb_name = None
    for ps in lines:
        tmp_tb_name = ps[0]
        if tb_name != tmp_tb_name:
            tb_name = tmp_tb_name
            sql += f"{tb_name} USING meters TAGS({ps[5]}, {ps[6]}) VALUES "
        sql += f"('{ps[1]}', {ps[2]}, {ps[3]}, {ps[4]}) "
    return sql


def insert_data(conn: taos.TaosConnection):
    sql = get_sql()
    affected_rows = conn.execute(sql)
    print("affected_rows", affected_rows)  # 8


if __name__ == '__main__':
    connection = get_connection()
    print(connection)

    try:
        create_stable(connection)
        insert_data(connection)
    finally:
        connection.close()

# def test_connection():
#     # all parameters are optional.
#     # if database is specified,
#     # then it must exist.
#     conn = taos.connect(host="localhost",
#                         port=6030,
#                         user="root",
#                         password="taosdata",
#                         database="test")
#     print('client info:', conn.client_info)
#     print('server info:', conn.server_info)
#     conn.close()
#
#
# if __name__ == "__main__":
#     test_connection()