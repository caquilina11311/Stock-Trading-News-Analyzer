from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Stock import loop
import pandas as pd
import asyncio 


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = ["*"],
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers =["*"],
)

@app.get("/data")
async def get_data():
    try:
        result = await loop()
        return result
    except Exception as e:
        return {"error":str(e)}