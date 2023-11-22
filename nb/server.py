import mtath_pb2_grpc, math_pb2
import grpc
from concurrent import futures
from cassandra import ConsistencyLevel

class StationServicer(object):
    """Missing associated documentation comment in .proto file."""

    def RecordTemps(self, request, context):
        """Missing associated documentation comment in .proto file."""
        station_id=request.station_id
        tmax=request.tmax
        tmin=request.tmin
        insert_statement=cass.prepare()
        insert_statement.consistency_level=ConsistencyLevel.ONE

    def StationMax(self, request, context):
        """Missing associated documentation comment in .proto file."""
        station_id=request.station_id
        
        max_statement = cass.prepare("????")
        max_statement.consistency_level = ????


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
server.add_insecure_port('0.0.0.0:5440')
server.start()
print("started")
server.wait_for_termination()
