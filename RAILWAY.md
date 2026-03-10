# Deploy ACR-KPIs Performance Dashboard on Railway

## 1. Push to GitHub

Ensure this repo is on GitHub (Railway deploys from Git).

## 2. Create a Railway project

1. Go to [railway.app](https://railway.app) and sign in.
2. **New Project** → **Deploy from GitHub repo**.
3. Select this repository.
4. Railway will detect Python and use `requirements.txt` and `railway.toml` (or `Procfile`).

## 3. Set environment variables

In your Railway service → **Variables**:

| Variable | Description |
|----------|-------------|
| `GOOGLE_APPLICATION_CREDENTIALS_JSON` | **Required.** Paste the full contents of your Google Cloud service account JSON key (the same as `keyy.json`). Use a single line or ensure newlines are preserved. |

To get the value: open your `keyy.json` file and copy the entire JSON object. In Railway, paste it as the variable value (you can use multiline).

## 4. Generate a domain

1. Open your service → **Settings** → **Networking**.
2. Click **Generate Domain**.
3. Your dashboard will be available at `https://<your-app>.up.railway.app`.

## 5. Deploy

- **From GitHub:** Push to your main branch; Railway will build and deploy automatically.
- **From CLI:** Install [Railway CLI](https://docs.railway.com/guides/cli), run `railway link` then `railway up`.

## Local development

- Use a local `keyy.json` in the project root (do not commit it).
- Run: `python -m uvicorn api:app --host 0.0.0.0 --port 8000`
- Open: http://localhost:8000
