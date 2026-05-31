import uvicorn

from src.gold.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "src.gold.serve:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )
