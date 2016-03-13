from pydrill.client import PyDrill

from DAL import queryDictionary
from pandas import DataFrame, Series
import pandas as pd
import numpy as np


class DataService:
    __drill = None
    __dictionary = queryDictionary.QueryDictionary

    def __init__(self):
        self.init_drill_connection();

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

    def get_users(self):
        query = self.__dictionary.get_users()
        results = self.__drill.query(query)
        return results

    def get_elite_users(self):
        query = self.__dictionary.get_elite()
        results = self.__drill.query(query)
        return results

    def get_elite_users_count(self):
        query = self.__dictionary.get_elite_count()
        results = self.__drill.query(query)
        return results

    def get_elite_users_tip(self):
        query = self.__dictionary.get_elite_tip()
        results = self.__drill.query(query)
        return results

    def get_elite_users_review(self):
        query = self.__dictionary.get_elite_review()
        results = self.__drill.query(query, 30)
        return results

    def get_restaurant_review(self):
        query = self.__dictionary.get_restaurant_reviews()
        results = self.__drill.query(query, 30)
        return results

    def get_users_with_reviews(self, categories='Restaurant'):
        query = self.__dictionary.get_restaurant_reviews2()
        results = self.__drill.query(query, 30)
        return results

    def test(self):
        query = self.__dictionary.get_elite_tip()
        results = self.__drill.query(query, 300)
        frame = DataFrame(data=results.rows, columns=results.columns)
        print(frame['tip_count'].describe())

    @staticmethod
    def get_frame(results, number_of_records=None):
        return DataFrame(data=results.rows, columns=results.columns)

    @staticmethod
    def print_frame(results, records_to_display=None):
        frame = DataService.get_frame(results, records_to_display)
        # frame = DataFrame(data=results.rows, columns=results.columns)
        print("\n")

        if records_to_display is None:
            print(frame.to_string(justify='right'))
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
        self.print_frame(self.get_elite_users(), 10)
        self.print_frame(self.get_elite_users_count(), 10)
        self.print_frame(self.get_elite_users_tip(), 10)
        self.print_frame(self.get_elite_users_review(), 10)
        self.print_frame(self.get_restaurant_review(), 10)
        self.print_frame(self.get_elite_users(), 10)
        # self.test()


if __name__ == '__main__':
    print('''Testing connection to Drill''')
    DataService().main()
