class QueryDictionary:
    @staticmethod
    def get_yelp_elite_query():
        yelp_elite = '''
         SELECT user_id, elite FROM
            `dfs.root`.`./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/yelp_academic_dataset_user.json`
         WHERE elite[0] IS NOT NULL  LIMIT 200
        '''
        return yelp_elite

    @staticmethod
    def get_yelp_elite_count_query():
        yelp_elite_count = '''
         SELECT count(*) FROM
            dfs.`/Users/Aymeric/apache-drill-1.5.0/YelpDataSet/yelp_academic_dataset_user.json`
        WHERE elite[0] IS NOT NULL  LIMIT 200
        '''
        return yelp_elite_count
