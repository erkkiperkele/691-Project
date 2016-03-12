from pydrill.client import PyDrill

from DAL import queryDictionary
from pandas import DataFrame, Series
import pandas as pd
import numpy as np

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
        results = self.__drill.query(query)
        self.print_frame(results, 10)

    def get_yelp_elite_count(self):
        query = self.__dictionary.get_elite_count()
        results = self.__drill.query(query)
        self.print_frame(results, 10)

    def get_yelp_elite_tip(self):
        query = self.__dictionary.get_elite_tip()
        results = self.__drill.query(query)
        self.print_frame(results, 10)

    def get_yelp_elite_review(self):
        query = self.__dictionary.get_elite_review()
        results = self.__drill.query(query, 30)
        self.print_frame(results, 10)

    def get_yelp_restaurant_review(self):
        query = self.__dictionary.get_restaurant_reviews()
        results = self.__drill.query(query, 30)
        self.print_frame(results, 10)

    def test(self):
        query = self.__dictionary.get_elite_tip()
        results = self.__drill.query(query, 30)
        frame = DataFrame(data=results.rows, columns=results.columns)
        print(frame['tip_count'].describe())

    @staticmethod
    def print_frame(results, records_to_display=None):
        frame = DataFrame(data=results.rows, columns=results.columns)
        print("\n")

        if records_to_display is None:
            print(frame.to_string(justify='left'))
        else:
            print(frame[:records_to_display].to_string(justify='left'))

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
        # self.get_yelp_restaurant_review()
        self.test()


if __name__ == '__main__':
    print('''Testing connection to Drill''')
    DataService().main()
