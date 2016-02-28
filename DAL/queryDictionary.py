def get_yelp_test():
    yelp_reviews = '''
      SELECT * FROM
      `dfs.root`.`./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/yelp_academic_dataset_user.json`
      LIMIT 5
    '''
    return yelp_reviews


def get_yelp_elite():
    yelp_elite = '''
     SELECT user_id, elite FROM
        `dfs.root`.`./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/yelp_academic_dataset_user.json`
     WHERE elite[0] IS NOT NULL  LIMIT 200
    '''
    return yelp_elite


def get_yelp_elite_count():
    yelp_elite_count = '''
     SELECT count(*) FROM
        dfs.`/Users/Aymeric/apache-drill-1.5.0/YelpDataSet/yelp_academic_dataset_user.json`
    WHERE elite[0] IS NOT NULL  LIMIT 200
    '''
    return yelp_elite_count

