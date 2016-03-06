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
        yelp_elite_query = self.__dictionary.get_yelp_elite_query()
        yelp_elite = self.__drill.query(yelp_elite_query)
        self.print_header("elite members")
        for result in yelp_elite:
            print("%s - %s" % (result['user_id'], result['elite']))

    @staticmethod
    def print_header(query_name):
        print('''
-----------------------
Printing %s:
-----------------------''' % query_name)

    def main(self):
        self.init_drill_connection()
        self.get_yelp_elite()


if __name__ == '__main__':
    print('''Testing connection to Drill''')
    DataService().main()
