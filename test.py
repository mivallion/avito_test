from common import get_query_count, get_top_ads, update_queries, update_top_ads

def test_get_query_count_correct_data():
    query = "Котята"
    loc_id = 637640
    res = get_query_count(query, loc_id)
    assert res != None

def test_get_query_count_without_location():
    query = "Котята"
    loc_id = None
    res = get_query_count(query, loc_id)
    assert res != None

def test_get_query_count_uncorrect_loc_id():
    query = "Котята"
    loc_id = 2125321412
    res = get_query_count(query, loc_id)
    assert res == None

def test_get_query_count_empty_query():
    query = ""
    loc_id = 637640
    res = get_query_count(query, loc_id)
    assert res != None

def test_update_queries():
    update_queries()

def test_update_top_ads():
    update_top_ads()

def test_get_top_ads_correct_data():
    query = "Котята"
    loc_id = 637640
    res = get_top_ads(query, loc_id, 5)
    assert res != None

def test_get_top_ads_without_location():
    query = "Котята"
    loc_id = None
    res = get_top_ads(query, loc_id, 5)
    assert res != None

def test_get_top_ads_uncorrect_loc_id():
    query = "Котята"
    loc_id = 637640
    res = get_top_ads(query, loc_id, 5)
    assert res == None