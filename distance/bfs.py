from DAL.dataService import DataService

### In progress. Not tested.
class BFS:
    '''
        Bi-directional search:
        https://en.wikipedia.org/wiki/Bidirectional_search
    '''

    __dataService = DataService()

    def calculate_distance(self, source, destination):
        midway_node = self.calculate_distance_bidirectional(source, destination)
        midway_distance = midway_node.calculate_path_length()
        return midway_distance * 2

    def calculate_distance_bidirectional(self, source, destination, depth: 0):
        source_children = source.extend()
        destination_children = destination.extend()

        for source_child in source_children:

            if self.is_contained(source_child, destination_children):
                return source_child

            if depth > 3:
                return None  # Would take too long to search whole graph

            else:
                for destination_child in destination_children:
                    return self.calculate_distance_bidirectional(source_child, destination_child, ++depth)

    @staticmethod
    def is_contained(item, destination_collection):
        for destination_item in destination_collection:
            if item.user_id == destination_item.user_id:
                return True


class YelpNode:
    __parent_node = None
    __user_review = None

    def __init__(self, user_review, parent_node):
        __parent_node = parent_node
        __user_review = user_review

    def extend(self, data_service):
        friends = []

        for friend_id in self.__user_review.friends_id:
            friend = data_service.get_user_review(friend_id)
            friend_node = YelpNode(friend, self)
            friends.append(friend_node)

        return friends

    def calculate_path_length(self):
        length = 0
        current_node = self

        while self.__parent_node != None:
            ++length

        return length
