# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import PostgreSQL as PgSQL


def get_queries():
    queries = []
    for i in range(22):
        with open("/mnt/hgfs/share/TPC-H_Tools_v3.0.0/dbgen/queries/" + i + ".sql") as f:
            query = f.read()
            queries.append(query)
    return queries


def get_query(index):
    with open("/mnt/hgfs/share/TPC-H_Tools_v3.0.0/dbgen/queries/" + index + ".sql") as f:
        query = f.read()
    return query


def add(max_reward, reward, max_reward_index, index):
    for m in range(len(max_reward)-1, -1, -1):
        if reward > max_reward[m]:
            if m == 0:
                max_reward.insert(0, reward)
                max_reward.pop()
                max_reward_index.insert(0, index)
                max_reward_index.pop()
            else:
                continue
        if m < len(max_reward)-1:
            max_reward.insert(m+1, reward)
            max_reward.pop()
            max_reward_index.insert(m+1, index)
            max_reward_index.pop()
    return max_reward, max_reward_index


def generate_candidate_single_column_index(per_query_index, query_number):
    pg = PgSQL.PGHypo()
    reward = []
    queries = get_queries()
    init_time_cost = pg.get_queries_cost(queries)
    init_space_cost = pg.get_storage_cost(queries)
    max_reward = [[0] * len(per_query_index) for _ in range(query_number)]
    max_reward_index = [0 for i in range(per_query_index)]
    for q in range(query_number):
        query = get_query(q)
        tables_name = query.split("from")[1].split("where")[0].split(",")
        attribute = []
        for table_name in tables_name:
            attribute = attribute + pg.get_attributes(table_name,"tpch")
        for i in range(len(attribute)):
            if pg.execute_create_hypo(attribute[i]) == 1:
                reward.append(((init_time_cost - pg.get_query_cost(query)) / init_time_cost)/
                              ((init_space_cost - pg.get_query_cost(query)) / init_space_cost))
            assert pg.execute_delete_hypo(attribute[i]) == 1
            for r in reward:
                result = add(max_reward[q],r,max_reward_index[q],reward.index(r))
                max_reward[q] = result[0]
                max_reward_index[q] = result[1]
    return max_reward_index


def generate_candidate_compound_index(per_query_index, query_number):
    pass


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
