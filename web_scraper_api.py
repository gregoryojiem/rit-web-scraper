import uvicorn
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis
from typing import List, Dict, Union
from scraper_to_vector_store import get_knowledge_source


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
    urls: Union[List[str], Dict[str, str]] = Field(
        description="Either a list of URLs or a dictionary mapping URLs to refresh times (e.g., '1 day', '2 hours')"
    )
    name: str = Field(description="Name for the vector store")


class VectorStoreResponse(BaseModel):
    vector_store_id: str


@app.post("/vector-store/", response_model=VectorStoreResponse)
async def create_vector_store(request: VectorStoreRequest):
    """
    Create or update a vector store from web content
    
    The URLs can be provided in two formats:
    - A simple list of URLs: ["https://example.com", "https://another.com"]
    - A dictionary mapping URLs to refresh times: {"https://example.com": "1 day", "https://another.com": "1 week"}
    
    Valid refresh time formats: "X minutes", "X hours", "X days", "X weeks", "X months", "X years"
    """
    vector_store_id = get_knowledge_source(request.urls, request.name)
    print("ID is: " + vector_store_id)
    return {
        'vector_store_id': vector_store_id
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
