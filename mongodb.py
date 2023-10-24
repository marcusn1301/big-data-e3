from pprint import pprint
from DbConnector import DbConnector

import os

from bson.objectid import ObjectId
import datetime

from haversine import haversine


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def insert_documents(self):
        # activity = {
        #     '_id': ObjectId(),
        #     'user_id': 181,
        #     'user_has_labels': False,
        #     'transportation_mode': '',
        #     'start_date_time': datetime.datetime(2009, 11, 12, 11, 14),
        #     'end_date_time': datetime.datetime(2009, 11, 12, 11, 14),
        #     'trackpoints': [
        #         ObjectId(),
        #         ObjectId(),
        #     ]
        # }

        # trackpoint: {
        #     '_id': ObjectId(),
        #     'lat': 0.0,
        #     'lon': 0.0,
        #     'altitude': 500,
        #     'date_days': 5.5,
        #     'date_time': datetime.datetime(2009, 11, 12, 11, 14),
        #     'activity_id': ObjectId()
        # }

        labeled_ids = []

        with open(f'./dataset/labeled_ids.txt') as labeled_ids_file:
            lines = labeled_ids_file.read().splitlines()
            labeled_ids += list(map(lambda l: int(l), lines))

        activityCollection = self.db['Activity']
        trackpointsCollection = self.db['Trackpoint']

        for userID in filter(lambda u: u.isnumeric(), os.listdir('./dataset/Data')):
            hasLabel = int(userID) in labeled_ids

            print('Inserting user', userID)

            # self.cursor.execute(f'INSERT INTO User VALUES ({int(userID)}, {hasLabel})')

            labels = []

            if hasLabel:
                with open(f'./dataset/Data/{userID}/labels.txt') as labelsFile:
                    lines = labelsFile.read().splitlines()[1:]

                    for line in lines:
                        words = line.split()

                        words[0] = words[0].replace('/', '-')
                        words[2] = words[2].replace('/', '-')

                        labels.append([datetime.datetime.fromisoformat('T'.join(
                            words[:2])), datetime.datetime.fromisoformat('T'.join(words[2:4])), words[4]])

            for activityID in map(lambda f: f.split('.')[0], os.listdir(f'./dataset/Data/{userID}/Trajectory')):
                with open(f'./dataset/Data/{userID}/Trajectory/{activityID}.plt') as pltFile:
                    lines = pltFile.read().splitlines()[6:]

                    # Skip filer med mer enn 2500 linjer
                    if len(lines) > 2500:
                        continue

                    startDateTime = datetime.datetime.fromisoformat(
                        'T'.join(lines[0].split(',')[-2:]))
                    endDateTime = datetime.datetime.fromisoformat(
                        'T'.join(lines[-1].split(',')[-2:]))

                    transportationMode = ''

                    for label in labels:
                        if label[0] == startDateTime and label[1] == endDateTime:
                            transportationMode = label[2]
                            break

                    activityObjectID = ObjectId()

                    trackpoints = []

                    for line in lines:
                        fields = line.split(',')
                        trackpoints.append({
                            '_id': ObjectId(),
                            'lat': float(fields[0]),
                            'lon': float(fields[1]),
                            # Ifølge oppgaven e altitude en int, men i datasettet e det en float, f.eks user 135...
                            'altitude': float(fields[3]),
                            'date_days': float(fields[4]),
                            'date_time': datetime.datetime.fromisoformat('T'.join(fields[-2:])),
                            'activity_id': activityObjectID
                        })

                    trackpointsCollection.insert_many(trackpoints)

                    activityCollection.insert_one({
                        '_id': activityObjectID,
                        'user_id': int(userID),
                        'user_has_label': hasLabel,
                        'transportation_mode': transportationMode,
                        'start_date_time': startDateTime,
                        'end_date_time': endDateTime,
                        'trackpoints': list(map(lambda t: t['_id'], trackpoints))
                    })

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = ExampleProgram()
        program.show_coll()

        # program.drop_coll(collection_name='Activity')
        # program.drop_coll(collection_name='Trackpoint')
        # # Check that the table is dropped
        # program.show_coll()
        # program.create_coll(collection_name='Activity')
        # program.create_coll(collection_name='Trackpoint')
        # program.insert_documents()
        # program.show_coll()

        activityCollection = program.db['Activity']
        trackpointsCollection = program.db['Trackpoint']

        # Aggregation e det komplekse queries i MongoDB består av: https://pymongo.readthedocs.io/en/stable/examples/aggregation.html
        # No inserte vi bare users som har trackpoints, mens i exercise 2 inserte vi alle users,
        # også dem som ikke har Activities.

        # print('Task 1')
        # userCount = len(activityCollection.distinct('user_id'))
        # activityCount = activityCollection.count_documents({})
        # trackpointCount = trackpointsCollection.count_documents({})
        # print('Users:', userCount)
        # print('Acitivities:', activityCount)
        # print('Trackpoints:', trackpointCount)

        # Users: 173
        # Acitivities: 16048
        # Trackpoints: 9681756

        # print('Task 2')
        # print('Average number of activities pr user:', activityCount/userCount)
        # Average number of activities pr user: 92.76300578034682

        # print('Task 3')
        # topUserActivities = [*activityCollection.aggregate([
        #     {"$group": {'_id': "$user_id", 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}},
        # ])][:20]

        # print('Top users:')
        # for user in topUserActivities:
        #     print(f'''ID: {user['_id']} Count: {user['count']}''')

        # print('Task 4')
        # userTaxi = [*activityCollection.aggregate([
        #     {'$match': {'transportation_mode': 'taxi'}},
        #     {"$group": {'_id': "$user_id"}},
        # ])]

        # print('Users who have taken a taxi:')
        # for user in userTaxi:
        #     print(f'''ID: {user['_id']}''')

        # print('Task 5')
        # transportationModes = [*activityCollection.aggregate([
        #     {'$match': {'transportation_mode': {'$ne': ''}}},
        #     {"$group": {'_id': "$transportation_mode", 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}}
        # ])]

        # print('Transportation modes and their occurences:')
        # for mode in transportationModes:
        #     print(f'''Mode: {mode['_id']} - Count: {mode['count']}''')

        # print('Task 6a')
        # yearActivities = [*activityCollection.aggregate([
        #     {"$group": {'_id': {'$year': '$start_date_time'}, 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}},
        #     {"$limit": 1}
        # ])]

        # print('Year with the most activities:')
        # for activity in yearActivities:
        #     print(f'''Year: {activity['_id']} - Count: {activity['count']}''')

        # print('Task 6b')
        # yearRecordedHours = [*activityCollection.aggregate([
        #     {'$addFields': {'hours': {'$dateDiff': {
        #         'startDate': '$start_date_time', 'endDate': '$end_date_time', 'unit': 'hour'}}}},
        #     {"$group": {'_id': {'$year': '$start_date_time'},
        #                 'hours': {'$sum': '$hours'}}},
        #     {"$sort": {"hours": -1}},
        #     {"$limit": 1}
        # ])]

        # print('Year with the most recorded hours:')
        # for activity in yearRecordedHours:
        #     print(f'''Year: {activity['_id']} - Hours: {activity['hours']}''')

        # print('Task 7')
        # walkActivityIDs = [activity['_id'] for activity in activityCollection.aggregate([
        #     {'$addFields': {'year': {'$year': '$start_date_time'}}},
        #     {'$match': {'user_id': 112, 'year': 2008, 'transportation_mode': 'walk'}},
        #     {'$project': {'_id': True}}
        # ])]

        # walkTrackpoints = [*trackpointsCollection.aggregate([
        #     {'$match': {'activity_id': {'$in': walkActivityIDs}}},
        #     {'$sort': {'date_time': 1}}
        # ])]

        # lastTP = None
        # totalDist = 0
        # for tp in walkTrackpoints:
        #     if lastTP and tp['activity_id'] == lastTP['activity_id']:
        #         totalDist += haversine((lastTP['lat'],
        #                                lastTP['lon']), (tp['lat'], tp['lon']))
        #     lastTP = tp

        # print('Total distance walked by user 112 in 2008:', totalDist)
        # # Total distance walked by user 112 in 2008: 115.47465961507991

        # print('Task 8')
        # activitiesToAltitudes = [*trackpointsCollection.aggregate([
        #     {'$match': {'altitude': {'$ne': -777}}},
        #     {"$group": {
        #         '_id': '$activity_id',
        #         'altitudes': {'$push': '$altitude'},
        #     }},
        #     {'$project': {'altitudes': True}},
        #     {'$lookup': {
        #         'from': 'Activity',
        #         'localField': '_id',
        #         'foreignField': '_id',
        #         'as': 'activity'
        #     }},
        #     # Dette e ganske sjuk syntax
        #     {'$project': {'altitudes': True, 'user_id': {'$first': '$activity.user_id'}}}
        # ])]

        # # print('Activities:', len(activitiesToAltitudes))
        # # # Activities: 16041
        # # # Det finnes 7 activities uten trackpoints tydeligvis...

        # userToAltitudeGained = dict.fromkeys(
        #     [a['user_id'] for a in activitiesToAltitudes], 0)
        # for activityToAltitudes in activitiesToAltitudes:
        #     lastAltitude = None
        #     for altitude in activityToAltitudes['altitudes']:
        #         if lastAltitude != None and altitude > lastAltitude:
        #             userToAltitudeGained[activityToAltitudes['user_id']
        #                                  ] += (altitude - lastAltitude)
        #         lastAltitude = altitude
        # userToAltitudeGained = dict(
        #     sorted(userToAltitudeGained.items(), key=lambda kv: kv[1], reverse=True))
        # userToAltitudeGained = {
        #     k: userToAltitudeGained[k] for k in list(userToAltitudeGained)[:20]}

        # for user in userToAltitudeGained:
        #     print(
        #         f'''ID: {user} - Altitude: {userToAltitudeGained.get(user)}''')

        # print('Task 9')
        # activityToTimestamps = [*trackpointsCollection.aggregate([
        #     {"$group" : {
        #         '_id': '$activity_id',
        #         'date_times': {'$push': '$date_time'},
        #     }},
        #     {'$lookup': {
        #         'from': 'Activity',
        #         'localField':'_id',
        #         'foreignField': '_id',
        #         'as': 'activity'
        #     }},
        #     {'$project': {'user_id': {'$first': '$activity.user_id'}, 'date_times': True}}
        # ])]

        # usersWithInvalidActivities = {}

        # for activityToTimestamp in activityToTimestamps:
        #     lastDateTime = None
        #     for dateTime in activityToTimestamp['date_times']:
        #         if lastDateTime != None and dateTime - lastDateTime >= datetime.timedelta(minutes=5):
        #             if activityToTimestamp['user_id'] in usersWithInvalidActivities:
        #                 usersWithInvalidActivities[activityToTimestamp['user_id']] += 1
        #             else:
        #                 usersWithInvalidActivities[activityToTimestamp['user_id']] = 1
        #             break
        #         lastDateTime = dateTime

        # print(f'Users with number of invalid activities: {usersWithInvalidActivities}')
        # # Users with number of invalid activities: {135: 5, 132: 3, 104: 97, 103: 24, 168: 19, 157: 9, 150: 16, 159: 5, 166: 2, 161: 7, 102: 13, 105: 9, 133: 4, 134: 31, 158: 9, 167: 134, 151: 1, 169: 9, 24: 27, 23: 11, 15: 46, 12: 43, 79: 2, 46: 13, 41: 201, 48: 1, 77: 3, 83: 15, 84: 99, 70: 5, 13: 29, 14: 118, 22: 55, 25: 263, 71: 29, 85: 184, 82: 27, 76: 8, 40: 17, 78: 19, 47: 6, 65: 26, 91: 63, 96: 35, 62: 249, 54: 2, 53: 7, 98: 5, 38: 58, 7: 30, 0: 101, 9: 31, 36: 34, 31: 3, 52: 44, 99: 11, 55: 15, 63: 8, 97: 14, 90: 3, 64: 7, 30: 112, 8: 16, 37: 100, 1: 45, 39: 147, 6: 17, 174: 54, 180: 2, 173: 5, 145: 5, 142: 52, 129: 6, 111: 26, 118: 3, 127: 4, 144: 157, 172: 9, 181: 14, 175: 4, 121: 4, 119: 22, 126: 105, 110: 17, 128: 720, 117: 3, 153: 557, 154: 14, 162: 9, 165: 2, 131: 10, 136: 6, 109: 3, 100: 3, 107: 1, 138: 10, 164: 6, 163: 233, 155: 30, 152: 2, 106: 3, 139: 12, 101: 46, 108: 5, 130: 8, 89: 40, 42: 55, 45: 7, 87: 3, 73: 18, 74: 19, 80: 6, 20: 20, 27: 2, 18: 27, 11: 32, 16: 20, 29: 25, 81: 16, 75: 6, 72: 2, 86: 5, 44: 32, 88: 11, 43: 21, 17: 129, 28: 36, 10: 50, 26: 18, 19: 31, 21: 7, 3: 179, 4: 219, 32: 12, 35: 23, 95: 4, 61: 12, 66: 6, 92: 101, 59: 5, 50: 8, 57: 16, 68: 139, 34: 88, 33: 2, 5: 45, 2: 98, 56: 7, 69: 6, 51: 36, 93: 4, 67: 33, 58: 13, 60: 1, 94: 16, 112: 67, 115: 58, 123: 3, 124: 4, 170: 2, 141: 1, 146: 7, 179: 28, 125: 25, 122: 6, 114: 3, 113: 1, 147: 30, 140: 86, 176: 8, 171: 3}

        # print('Task 10')
        # forbiddenUsers = list(trackpointsCollection.aggregate([
        #     {'$project': {
        #         'lat': {'$round': ['$lat', 3]},
        #         'lon': {'$round': ['$lon', 3]},
        #         'activity_id': True
        #     }},
        #     {'$match': {
        #         'lat': 39.916,
        #         'lon': 116.397
        #     }},
        #     {'$lookup': {
        #         'from': 'Activity',
        #         'localField':'activity_id',
        #         'foreignField': '_id',
        #         'as': 'activity'
        #     }},
        #     {'$group': {'_id': {'$first': '$activity.user_id'}}}
        # ]))
        # print([u['_id'] for u in forbiddenUsers])
        # # [4, 131, 18]

        # print('Task 11')
        # usersTransportation = list(activityCollection.aggregate([
        #     {'$match': {'transportation_mode': {'$ne': ''}}},
        #     {"$group": {'_id': '$user_id', 'top_mode': {
        #         '$first': '$transportation_mode'}, 'count': {'$sum': 1}}},
        #     {"$sort": {"_id": 1}},
        # ]))

        # print('Users and their most used transportation mode:')
        # for u in usersTransportation:
        #     print(f'''(ID: {u['_id']}, Mode: {u['top_mode']}, Count: {u['count']})''')

    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
