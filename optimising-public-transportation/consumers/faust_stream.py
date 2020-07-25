"""Defines trends calculations for stations"""
import faust
import logging

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("connect-cta", value_type=Station)
out_topic = app.topic("com.udacity.cta", partitions=1)
table = app.Table(
    "cta-table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transform_station_event(stations):
    async for station in stations:#.group_by(Station.station_id):# do i need to do group_by ?
        if station.red == True:
            line = 'red'
        elif station.blue == True:
            line = 'blue'
        else:
            line = 'green'
            
        transformed_station = TransformedStation(
            station.station_id,
            station.station_name,
            station.order,
            line
        )
        table[station.station_id] = transformed_station
    

if __name__ == "__main__":
    app.main()
