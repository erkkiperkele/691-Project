from pandas import DataFrame, pandas
import json

from DAL.dataService import DataService


class DataBuilder:
    __dataService = DataService();

    def build_feature_set_1(self, category='Restaurants'):
        """Feature Set 1
         • Total reviews : Total number of reviews written by user
         • Category reviews : Number of reviews written by user for businesses in the category • Average rating by user for businesses in the category
         • Standard dev of ratings by user for businesses in the category
         • Funny : Number of funny votes user got for all her reviews in this category
         • Useful : Number of useful votes user got for all her reviews in this category
         • Cool : Number of cool votes user got for all her reviews in this category
         • Number of unique business in category reviewed by user
         • Months : Number of months for which user has been yelping"""

        # TODO: add votes and yelping since in month.
        return self.__dataService.get_featureset1_but_votes(category)

    def test(self):
        frame = self.__dataService.get_frame(self.build_feature_set_1())
        print(frame.describe())
        frame.to_json('/Users/Aymeric/Desktop/featureset1_but_votes.json')
        df = pandas.read_json('/Users/Aymeric/Desktop/featureset1_but_votes.json')
        print(df.describe())
        print(df.to_string())


    def main(self):
        self.test()

if __name__ == '__main__':
    DataBuilder().main()


