from datetime import timedelta
import faust
from data import data


app = faust.App('homework.stream.v2', broker='kafka://localhost:9092')
topic = app.topic('homework', value_type=data)

homework = app.Table('homework_windowed', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(data.vendorId):
        homework[event.vendorId] += event.example_value


if __name__ == '__main__':
    app.main()