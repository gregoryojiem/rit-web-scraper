import uvicorn
import uuid
from pydantic import BaseModel
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis
from vector_store_util import get_or_make_vector_store
from main import download_static_website
from typing import List

@asynccontextmanager
async def lifespan(application: FastAPI):
    redis = aioredis.from_url("redis://localhost:6379", encoding="utf8", decode_responses=False)
    FastAPICache.init(RedisBackend(redis), prefix="resampbot-cache")
    yield
    await redis.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://gregoryojiem.github.io", "http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class VectorStoreRequest(BaseModel):
    urls: List[str]
    name: str

class VectorStoreResponse(BaseModel):
    vector_store_id: str

@app.post("/vector-store/", response_model=VectorStoreResponse)
async def create_vector_store(request: VectorStoreRequest):
    new_base_path = download_static_website(request.urls[0], convert_html_to_markdown=True, end_markdown_with_txt_extension=True)
    print("Web scraping complete!")
    path = "output" + "/" + new_base_path.split("/")[0]
    vector_store_id = get_or_make_vector_store(path, request.name)
    print("New id is: " + vector_store_id)
    return {
        'vector_store_id': vector_store_id
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
