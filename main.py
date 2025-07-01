"""Entry point for launching the FastAPI application.

Run ``python main.py`` to start a local server.
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000)

