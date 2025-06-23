from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
from app.api.automation import router as automation_router
from fastapi.responses import JSONResponse
from app.config import settings
import httpx

app = FastAPI()

# Configure templates and static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers (API routes only are protected)
app.include_router(
    automation_router,
    prefix="/api/v1"
)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.get("/")
async def root(request: Request):
    # Redirect to dashboard directly
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/dashboard")
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/login")
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/api/subaccounts")
def get_subaccounts():
    return JSONResponse(content=settings.subaccounts_list)

@app.get("/api/subaccounts/{sub_id}/pipelines")
async def get_pipelines_for_subaccount(sub_id: str):
    subaccounts = settings.subaccounts_list
    sub = next((s for s in subaccounts if str(s.get("id")) == str(sub_id)), None)
    if not sub or not sub.get("api_key"):
        return []
    api_key = sub["api_key"]
    url = "https://rest.gohighlevel.com/v1/pipelines"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with httpx.AsyncClient(timeout=settings.ghl_api_timeout) as client:
        try:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            return data.get("pipelines", [])
        except Exception as e:
            # Log the error and return an empty list instead of raising
            import logging
            logging.error(f"Failed to fetch pipelines for subaccount {sub_id}: {e}")
            return []