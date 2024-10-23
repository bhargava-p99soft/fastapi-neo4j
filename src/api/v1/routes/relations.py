
# def delete_relations(deletion_label:str, deletion_id_title: str, deletion_id_value:str, relation: str, db: Neo4jConnection = Depends(get_db)):
#     query = """Match (a:  {$deletion_id_title})-[r:{relation: $relation}]-(b}) delete r"""
#     result = db.query(query, {"deletion_item": deletion_item, "relation": relation})
#     if result:
#         return False
#     else:
#         return True