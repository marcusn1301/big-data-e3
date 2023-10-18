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

                        labels.append([datetime.datetime.fromisoformat('T'.join(words[:2])), datetime.datetime.fromisoformat('T'.join(words[2:4])), words[4]])

            for activityID in map(lambda f: f.split('.')[0], os.listdir(f'./dataset/Data/{userID}/Trajectory')):
                with open(f'./dataset/Data/{userID}/Trajectory/{activityID}.plt') as pltFile:
                    lines = pltFile.read().splitlines()[6:]

                    # Skip filer med mer enn 2500 linjer
                    if len(lines) > 2500:
                        continue

                    startDateTime = datetime.datetime.fromisoformat('T'.join(lines[0].split(',')[-2:]))
                    endDateTime = datetime.datetime.fromisoformat('T'.join(lines[-1].split(',')[-2:]))

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
                            'altitude': float(fields[3]), # Ifølge oppgaven e altitude en int, men i datasettet e det en float, f.eks user 135...
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
        # # Average number of activities pr user: 92.76300578034682

        # print('Task 3')
        # print([*activityCollection.aggregate([
        #     {"$group" : {'_id': "$user_id", 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}},
        # ])][:20])
        # # [{'_id': 128, 'count': 2102}, {'_id': 153, 'count': 1793}, {'_id': 25, 'count': 715}, {'_id': 163, 'count': 704}, {'_id': 62, 'count': 691}, {'_id': 144, 'count': 563}, {'_id': 41, 'count': 399}, {'_id': 85, 'count': 364}, {'_id': 4, 'count': 346}, {'_id': 140, 'count': 345}, {'_id': 167, 'count': 320}, {'_id': 68, 'count': 280}, {'_id': 17, 'count': 265}, {'_id': 3, 'count': 261}, {'_id': 14, 'count': 236}, {'_id': 126, 'count': 215}, {'_id': 30, 'count': 210}, {'_id': 112, 'count': 208}, {'_id': 11, 'count': 201}, {'_id': 39, 'count': 198}]
        
        # print('Task 4')
        # print([*activityCollection.aggregate([
        #     {'$match': {'transportation_mode': 'taxi'}},
        #     {"$group" : {'_id': "$user_id"}},
        # ])])
        # # [{'_id': 10}, {'_id': 85}, {'_id': 62}, {'_id': 58}, {'_id': 128}, {'_id': 78}, {'_id': 163}, {'_id': 80}, {'_id': 111}, {'_id': 98}]

        # print('Task 5')
        # print([*activityCollection.aggregate([
        #     {'$match': {'transportation_mode': {'$ne': ''}}},
        #     {"$group" : {'_id': "$transportation_mode", 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}}
        # ])])
        # # [{'_id': 'walk', 'count': 481}, {'_id': 'car', 'count': 419}, {'_id': 'bike', 'count': 262}, {'_id': 'bus', 'count': 199}, {'_id': 'subway', 'count': 133}, {'_id': 'taxi', 'count': 37}, {'_id': 'airplane', 'count': 3}, {'_id': 'train', 'count': 2}, {'_id': 'run', 'count': 1}, {'_id': 'boat', 'count': 1}]

        # print('Task 6a')
        # print([*activityCollection.aggregate([
        #     {"$group" : {'_id': {'$year': '$start_date_time'}, 'count': {'$sum': 1}}},
        #     {"$sort": {"count": -1}}
        # ])])
        # # [{'_id': 2008, 'count': 5895}, {'_id': 2009, 'count': 5879}, {'_id': 2010, 'count': 1487}, {'_id': 2011, 'count': 1204}, {'_id': 2007, 'count': 994}, {'_id': 2012, 'count': 588}, {'_id': 2000, 'count': 1}]

        # print('Task 6b')
        # print([*activityCollection.aggregate([
        #     {'$addFields': {'hours': {'$dateDiff': {'startDate': '$start_date_time', 'endDate': '$end_date_time', 'unit': 'hour'}}}},
        #     {"$group" : {'_id': {'$year': '$start_date_time'}, 'hours': {'$sum': '$hours'}}},
        #     {"$sort": {"hours": -1}}
        # ])])
        # # [{'_id': 2009, 'hours': 11636}, {'_id': 2008, 'hours': 9105}, {'_id': 2007, 'hours': 2324}, {'_id': 2010, 'hours': 1432}, {'_id': 2011, 'hours': 1130}, {'_id': 2012, 'hours': 721}, {'_id': 2000, 'hours': 0}
        
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
        #         totalDist += haversine((lastTP['lat'], lastTP['lon']), (tp['lat'], tp['lon']))
        #     lastTP = tp

        # print('Total distance walked by user 112 in 2008:', totalDist)
        # # Total distance walked by user 112 in 2008: 115.47465961507991

        # print('Task 8')
        # activitiesToAltitudes = [*trackpointsCollection.aggregate([
        #     {'$match': {'altitude': {'$ne': -777}}},
        #     {"$group" : {
        #         '_id': '$activity_id', 
        #         'altitudes': {'$push': '$altitude'},
        #     }},
        #     {'$project': {'altitudes': True}},
        #     {'$lookup': {
        #         'from': 'Activity',
        #         'localField':'_id',
        #         'foreignField': '_id',
        #         'as': 'activity'
        #     }},
        #     {'$project': {'altitudes': True, 'user_id': {'$first': '$activity.user_id'}}} # Dette e ganske sjuk syntax
        # ])]

        # # print('Activities:', len(activitiesToAltitudes))
        # # # Activities: 16041
        # # # Det finnes 7 activities uten trackpoints tydeligvis...

        # userToAltitudeGained = dict.fromkeys([a['user_id'] for a in activitiesToAltitudes], 0)
        # for activityToAltitudes in activitiesToAltitudes:
        #     lastAltitude = None
        #     for altitude in activityToAltitudes['altitudes']:
        #         if lastAltitude != None and altitude > lastAltitude:
        #             userToAltitudeGained[activityToAltitudes['user_id']] += (altitude - lastAltitude)
        #         lastAltitude = altitude
        # userToAltitudeGained = dict(sorted(userToAltitudeGained.items(), key=lambda kv: kv[1], reverse=True))
        # userToAltitudeGained = {k: userToAltitudeGained[k] for k in list(userToAltitudeGained)[:20]}
        # print(userToAltitudeGained)
        # # {128: 2135669.282417741, 153: 1820736.9522737002, 4: 1089358.0, 41: 789924.1000003539, 3: 766613.0, 85: 714053.1000000071, 163: 673472.3440420027, 62: 596106.5999999233, 144: 588718.9123359431, 30: 576377.0, 39: 481311.0, 84: 430319.0, 0: 398638.0, 2: 377503.0, 167: 370650.1136482952, 25: 358131.7999999046, 37: 325572.79999995086, 140: 311175.52283458825, 126: 272394.47427820024, 17: 205319.39999998698}

    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
