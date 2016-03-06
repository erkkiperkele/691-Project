from pydrill.client import PyDrill

from DAL import queryDictionary


class DataService:
    __drill = None
    __dictionary = queryDictionary.QueryDictionary

    def init_drill_connection(self):
        self.__drill = PyDrill(host='localhost')
        is_drill_active = self.__drill.is_active()

        if is_drill_active:
            print("Drill is active: %s" % is_drill_active)
        else:
            print('''
            Drill is not active. Start your server in a terminal using command:
            cd /Users/Aymeric/apache-drill-1.5.0
            bin/drill-embedded
                ''')

    def get_yelp_elite(self):
        query = self.__dictionary.get_elite()
        yelp_elite = self.__drill.query(query)

        self.print_header("elite members")
        for result in yelp_elite:
            print("%s - %s" % (result['user_id'], result['elite']))

    def get_yelp_elite_count(self):
        query = self.__dictionary.get_elite_count()
        yelp_elite = self.__drill.query(query)

        for result in yelp_elite:
            print("elite count: {0}".format(result['elite_count']))

    def get_yelp_elite_tip(self):
        query = self.__dictionary.get_elite_tip()
        yelp_elite_tip = self.__drill.query(query)

        self.print_header("elite extended members")
        print("user_id \t\t\t\t tip_count")
        for result in yelp_elite_tip:
            print("%s \t %s" % (result['user_id'], result['tip_count']))

        self.print_count(yelp_elite_tip)

    def get_yelp_elite_review(self):
        query = self.__dictionary.get_elite_review()
        yelp_elite_review = self.__drill.query(query, 30)

        self.print_header("elite extended members")
        print("user_id \t\t\t\t review_count")
        for result in yelp_elite_review:
            print("%s \t %s" % (result['user_id'], result['review_count']))

        self.print_count(yelp_elite_review)

    def get_yelp_restaurant_review(self):
        query = self.__dictionary.get_restaurant_reviews()
        yelp_restaurant_review = self.__drill.query(query, 30)

        self.print_header("elite restaurant reviews")
        print("user_id \t\t\t\t review_count \t\t avg_rating")
        for result in yelp_restaurant_review:
            print("%s \t %s \t\t\t\t\t %s" % (result['user_id'], result['reviews_count'], result['avg_rating']))

        self.print_count(yelp_restaurant_review)

    @staticmethod
    def print_header(query_name):
        print('''
-----------------------
Printing %s:
-----------------------''' % query_name)

    @staticmethod
    def print_count(records):
        print("Number of records: {0}".format(len(records.rows)))

    def main(self):
        self.init_drill_connection()
        # self.get_yelp_elite()
        # self.get_yelp_elite_count()
        # self.get_yelp_elite_tip()
        # self.get_yelp_elite_review()
        self.get_yelp_restaurant_review()


if __name__ == '__main__':
    print('''Testing connection to Drill''')
    DataService().main()
