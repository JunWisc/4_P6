import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
import cassandra
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from datetime import datetime

class temp_minmax:

    def __init__(self, tmin, tmax):
        self.tmax = tmax
        self.tmin = tmin

class StationServicer(station_pb2_grpc.StationServicer):

    def __init__(self):
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.session = cluster.connect('weather')
        self.insert_statement = self.session.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, ?)")
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.session.prepare("SELECT MAX(record.tmax) FROM weather.stations WHERE id = ?")
        self.max_statement.consistency_level = ConsistencyLevel.ALL
        self.session.cluster.register_user_type('weather','station_record',temp_minmax)
    def RecordTemps(self, request, context):
        try:
            tmm = temp_minmax(tmin= request.tmin, tmax= request.tmax)
            formatted_date = datetime.strptime(request.date, '%Y%m%d')
            self.session.execute(self.insert_statement, (request.station, formatted_date,tmm))
            return station_pb2.RecordTempsReply(error="")
        except cassandra.Unavailable as ue:
            error_message = f"need {ue.required_replicas} replicas, but only have {ue.alive_replicas}"
            return station_pb2.RecordTempsReply(error=error_message)
        except cassandra.cluster.NoHostAvailable as ne:
            for inner_error in ne.errors.values():
                if isinstance(inner_error, cassandra.Unavailable):
                    error_message = f"need {inner_error.required_replicas} replicas, but only have {inner_error.alive_replicas}"
                    return station_pb2.RecordTempsReply(error=error_message)
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

  #      self.session.execute(insert_statement, (station_id, tmax, tmin))
   #     return station_pb2.RecordTempsResponse()
    def StationMax(self, request, context):
        try:
            respone = self.session.execute(self.max_statement, [request.station])
            r=respone.one()
            if r:
                return station_pb2.StationMaxReply(tmax=r[0], error="")
        except cassandra.Unavailable as e:
            error_message = f"need {e.required_replicas} replicas, but only have {e.alive_replicas}"
            return station_pb2.StationMaxReply(tmax=0,error=error_message)
        except cassandra.cluster.NoHostAvailable as e:
            for inner_error in e.errors.values():
                if isinstance(inner_error, cassandra.Unavailable):
                    error_message = f"need {inner_error.required_replicas} replicas, but only have {inner_error.alive_replicas}"
                    return station_pb2.StationMaxReply(tmax=0,error=error_message)
        except Exception as e:
            return station_pb2.StationMaxReply(tmax=0, error=str(e))


#        max_tmax = max_result[0].max_tmax

 #       return station_pb2.StationMaxReply(max_tmax=max_tmax)
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
