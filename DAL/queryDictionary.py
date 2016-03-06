class QueryDictionary:
    """
    This class contains the base queries for the yelp dataset
    :__dataPath: the root folder for the yelp dataset json files
    """
    __dataPath = '''`dfs.root`.`./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/'''

    __user = {'user': __dataPath + '''yelp_academic_dataset_user.json`'''}
    __business = {'business': __dataPath + '''yelp_academic_dataset_business.json`'''}
    __checkin = {'checkin': __dataPath + '''yelp_academic_dataset_checkin.json`'''}
    __review = {'review': __dataPath + '''yelp_academic_dataset_review.json`'''}
    __tip = {'tip': __dataPath + '''yelp_academic_dataset_tip.json`'''}

    @classmethod
    def get_yelp_elite_query(cls):
        yelp_elite = '''
         SELECT user_id, elite FROM
            {user}
         WHERE elite[0] IS NOT NULL  LIMIT 200
        '''.format(**cls.__user)

        return yelp_elite

    @classmethod
    def get_yelp_elite_count_query(cls):
        yelp_elite_count = '''
         SELECT count(*) FROM
            {user}
        WHERE elite[0] IS NOT NULL  LIMIT 200
        '''.format(cls.__user)
        return yelp_elite_count
