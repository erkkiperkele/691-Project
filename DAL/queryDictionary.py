class QueryDictionary:
    """
    This class contains the base queries for the yelp dataset
    :__dataPath: the root folder for the yelp dataset json files
    :__tables: a dictionary of named arguments for each json file (table) to be passed to the queries
    """
    __dataPath = '''`dfs.root`.`./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/'''

    __tables = {
        'user': __dataPath + '''yelp_academic_dataset_user.json`''',
        'business': __dataPath + '''yelp_academic_dataset_business.json`''',
        'checkin': __dataPath + '''yelp_academic_dataset_checkin.json`''',
        'review': __dataPath + '''yelp_academic_dataset_review.json`''',
        'tip': __dataPath + '''yelp_academic_dataset_tip.json`'''
    }

    @classmethod
    def get_elite(cls):
        yelp_elite = '''
                 SELECT user_id, elite FROM {user}
                 WHERE elite[0] IS NOT NULL  LIMIT 200
        '''.format(**cls.__tables)

        return yelp_elite

    @classmethod
    def get_elite_count(cls):
        yelp_elite_count = '''
                SELECT count(*) as elite_count FROM {user}
                WHERE elite[0] IS NOT NULL
        '''.format(**cls.__tables)
        return yelp_elite_count

    @classmethod
    def get_elite_tip(cls):

        yelp_elite = '''
                SELECT u.user_id, count(*) as tip_count  FROM {user} u
                   JOIN {tip} as t
                       on u.user_id = t.user_id
                WHERE u.elite[0] IS NOT NULL
                GROUP BY u.user_id
                LIMIT 200
        '''.format(**cls.__tables)
        return yelp_elite

    @classmethod
    def get_elite_review(cls):

        yelp_elite = '''
                SELECT u.user_id, count(*) as review_count FROM {user} u
                   JOIN {review} as r
                       on u.user_id = r.user_id
                WHERE u.elite[0] IS NOT NULL
                GROUP BY u.user_id
                LIMIT 50000
        '''.format(**cls.__tables)
        return yelp_elite
