class QueryDictionary:
    """
    This class contains the base queries for the yelp dataset
    :__dataPath: the root folder for the yelp dataset json files
    :__tables: a dictionary of named arguments for each json file (table) to be passed to the queries
    """

    import os

    # # All
    __dataPath = '''`dfs.root`.`''' + os.path.normpath('./Users/Aymeric/apache-drill-1.5.0/YelpDataSet/')
    __suffixe = '''/yelp_academic_dataset'''
    __tables = {
        'user': __dataPath + __suffixe + '''_user.json`''',
        'business': __dataPath + __suffixe + '''_business.json`''',
        'checkin': __dataPath + __suffixe + '''_checkin.json`''',
        'review': __dataPath + __suffixe + '''_review.json`''',
        'tip': __dataPath + __suffixe + '''_tip.json`'''
    }

    # # Montréal
    # __dataPath = '''`dfs.root`.`''' + os.path.abspath('./../montreal_subset/')
    # __suffixe = '''/montreal'''
    #
    # __tables = {
    #     'user': __dataPath + __suffixe + '''_users_who_reviewed.json`''',
    #     'business': __dataPath + __suffixe + '''_business.json`''',
    #     'checkin': __dataPath + __suffixe + '''_checkin.json`''',
    #     'review': __dataPath + __suffixe + '''_reviews.json`''',
    #     'tip': __dataPath + __suffixe + '''_tip.json`''',
    #     'users_tip': __dataPath + __suffixe + '''_users_who_tipped.json`'''
    # }

    @classmethod
    def get_users(cls):
        users = '''
                 SELECT * FROM {user}
        '''.format(**cls.__tables)
        return users

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

    @classmethod
    def get_restaurant_reviews(cls):

        yelp_elite = '''
                SELECT
                    u.user_id,
                    count(rest.review_id) as reviews_count,
                    avg(rest.stars) as avg_rating
                FROM {user} u
                    JOIN (
                        SELECT r.user_id as user_id, r.review_id, r.stars
                        FROM {review} r
                            JOIN {business} as b
                                ON b.business_id = r.business_id
                        WHERE REPEATED_CONTAINS(b.categories,'Restaurants')
                        ) AS rest
                    ON u.user_id = rest.user_id
                WHERE u.elite[0] IS NOT NULL
                GROUP BY u.user_id
                LIMIT 10
        '''.format(**cls.__tables)
        return yelp_elite

    @classmethod
    def get_restaurant_reviews2(cls, category='Restaurants'):
        arg = {'category': category}
        cls.__tables.update(**arg)
        yelp_elite = '''
SELECT
    u.user_id,
    all_reviews.total_reviews,
    rest.categories_reviews,
    rest.avg_rating AS categories_avg_rating,
    rest.std_rating AS std_dev_rating,
    rest.biz_count AS categories_biz_count
FROM {user} u
    JOIN (
        SELECT rev.user_id, COUNT(*) as total_reviews
        FROM {review} rev
        GROUP BY rev.user_id
        ) AS all_reviews
    ON u.user_id = all_reviews.user_id
    JOIN (
        SELECT
            r.user_id AS user_id,
            COUNT(r.review_id) AS categories_reviews,
            AVG(r.stars) AS avg_rating,
            STDDEV_POP(r.stars) AS std_rating,
            COUNT(r.business_id) AS biz_count
        FROM {review} r
            JOIN {business} as b
                ON b.business_id = r.business_id
        WHERE REPEATED_CONTAINS(b.categories, 'Restaurants')
        GROUP BY r.user_id
        ) AS rest
    ON u.user_id = rest.user_id

        '''.format(arg, **cls.__tables)
        return yelp_elite

