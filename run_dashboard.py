"""Run the ACR-KPIs Performance Dashboard API (serves static/dashboard.html at /)."""
import os
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("api:app", host="0.0.0.0", port=port)
