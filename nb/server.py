import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

class StationServicer(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.session = self.cluster.connect()
        self.insert_statement = self.session.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, ?)")
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.session.prepare("SELECT MAX(record.tmax) FROM weather.stations WHERE id = ?")
        self.max_statement.consistency_level = ConsistencyLevel.TWO

    def RecordTemps(self, request, context):
        try:
            self.session.execute(self.insert_statement, (request.id, request.date, {'tmin': request.tmin, 'tmax': request.tmax}))
            return station_pb2.RecordTempsReply(error="")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:
            rows = self.session.execute(self.max_statement, [request.id])
            return station_pb2.StationMaxReply(tmax=rows[0][0], error="")
        except Exception as e:
            return station_pb2.StationMaxReply(tmax=0, error=str(e))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
