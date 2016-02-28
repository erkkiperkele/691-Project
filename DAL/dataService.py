# cd /Users/Aymeric/apache-drill-1.5.0
# bin/drill-embedded


from pydrill.client import PyDrill

from DAL import queryDictionary

drill = PyDrill(host='localhost')
is_drill_active = drill.is_active()

if is_drill_active:
    print("Drill is active: %s" % is_drill_active)
else:
    print('''
Drill is not active. Start your server in a terminal using command:
bin/drill-embedded
    ''')

yelp_elite = queryDictionary.get_yelp_elite()
yelp_reviews = drill.query(yelp_elite)

for result in yelp_reviews:
    print("%s - %s" % (result['user_id'], result['elite']))
