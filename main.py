import asyncio
import json
import datetime
from typing import Optional
from fastapi import BackgroundTasks, FastAPI
from producer import publish
import sys
app = FastAPI()

@app.on_event("startup")
async def schedule_periodic():
    loop = asyncio.get_event_loop()
    try:
        print("HI")
        # asyncio.ensure_future(producer_consume())
        # OR
        # loop.create_task(producer_consume())
        #  loop.run_until_complete(publish())
        # loop.run_forever()
    except KeyboardInterrupt:
        print(" EXECPTION HERE")
        sys.exit(0)
    
        # loop.close()

@app.get("/")
def home():
    return {"message": "Hello! , Welcome to my App."}


@app.post("/api/v1/product")
async def api_product(name: str, price: float, background_tasks: BackgroundTasks, description: Optional[str] = None):
    start_time = datetime.datetime.now()
    data = [1,2,3,4,5]
    # await sendDataToConsumer(data)
    background_tasks.add_task(publish)
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = f'{round(time_diff.total_seconds() * 1000)} ms'
    # return
    return {'message': 'Success', 'data': data, 'execution_time': execution_time}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app)