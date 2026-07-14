# Hostinger VPS Deploy — AI Cargo Backend (Pathwise-style)

Same pattern as Pathwise:

- **Frontend** stays on Vercel
- **Backend** runs as Docker on Hostinger VPS
- **No custom domain required** — use the VPS public IP + port, e.g. `http://YOUR_VPS_IP:8000`

---

## What I already prepared locally (NOT committed / NOT pushed)

On branch `main` (synced with GitHub), these are **local-only** until you commit:

| File | Purpose |
|------|---------|
| `Dockerfile` | Container image for FastAPI |
| `docker-compose.yml` | One-command start |
| `requirements.docker.txt` | Linux deps (+ CPU torch for compliance RAG) |
| `.dockerignore` | Smaller builds |
| `.env.example` | Template for secrets |
| `deploy/nginx-ai-cargo.conf` | **Optional** (only if you later add a domain) |
| `HOSTINGER_DEPLOY.md` | This guide |

Remotes on this machine:

- `origin` → `nsumesh/SmithAgenticAIChallenge`
- `mine` → `rahul370139/SmithAgenticAIChallenge` (your fork, same `main` tip)

**No commit and no push were made from Cursor.**

---

## 0. Prerequisites

| Need | Why |
|------|-----|
| Hostinger **VPS** + SSH | Same as Pathwise |
| **4 GB RAM recommended** (2 GB min) | XGBoost + sentence-transformers |
| VPS **public IP** | This becomes your backend URL |
| Firewall allows **TCP 8000** | So Vercel / browsers can reach the API |

---

## 1. Create `.env.production` (local, then copy to VPS)

```bash
cd "/Users/rahul/Downloads/Code scripts/AI_cargo"
cp .env.example .env.production
# fill Supabase / Groq / Gmail / Slack — same values as Railway
# never commit .env.production
```

---

## 2. Open port 8000 on Hostinger (Pathwise style)

In Hostinger VPS panel → **Firewall** (or ufw on the box):

```bash
# on VPS
sudo ufw allow 22/tcp
sudo ufw allow 8000/tcp
sudo ufw enable   # if not already
sudo ufw status
```

You want inbound **8000/tcp** open. No nginx / no domain needed for this mode.

---

## 3. Put code on the VPS

**Option A — rsync (keeps your local Hostinger files even before you push to GitHub)**

```bash
# on your Mac
rsync -avz --exclude '.venv' --exclude 'dashboard/node_modules' --exclude '.git' \
  --exclude '.env' \
  "/Users/rahul/Downloads/Code scripts/AI_cargo/" \
  root@YOUR_VPS_IP:/opt/ai-cargo/
```

Then SSH and create secrets on the server:

```bash
ssh root@YOUR_VPS_IP
cd /opt/ai-cargo
nano .env.production   # paste secrets
```

**Option B — after you personally push Hostinger files to GitHub**

```bash
# on VPS
git clone https://github.com/rahul370139/SmithAgenticAIChallenge.git /opt/ai-cargo
cd /opt/ai-cargo
nano .env.production
```

Until you push, use **Option A (rsync)**.

---

## 4. Build & run (Docker)

```bash
# on VPS
cd /opt/ai-cargo

# if Docker missing (Pathwise may already have it):
# curl -fsSL https://get.docker.com | sh

docker compose up -d --build
docker compose ps
docker compose logs -f api
```

Local-on-VPS smoke test:

```bash
curl -s "http://127.0.0.1:8000/api/windows?limit=1" | head
```

Public smoke test (from your Mac):

```bash
curl -s "http://YOUR_VPS_IP:8000/api/windows?limit=1" | head
```

Your backend URL (Pathwise style):

```text
http://YOUR_VPS_IP:8000
```

Example shape: `http://194.x.x.x:8000` — **not** `http://127.0.0.1:8000` in Vercel.
`127.0.0.1` only works when the browser is on the same machine as the API.

---

## 5. Point Vercel at the Hostinger IP

Vercel project → **Settings → Environment Variables**:

| Name | Value |
|------|--------|
| `VITE_API_URL` | `http://YOUR_VPS_IP:8000` |

No trailing slash. Then **Redeploy** the frontend (Vite bakes env at build time).

Dashboard already reads:

```js
const BASE = (import.meta.env.VITE_API_URL ?? '') + '/api';
```

CORS already allows `https://*.vercel.app`, so the Vercel frontend can call your IP API.

### Important browser note (HTTPS → HTTP)

Vercel sites are **HTTPS**. Calling an **HTTP** IP API can be blocked as **mixed content** in some browsers.

Pathwise worked this way for many setups; if your dashboard shows network / mixed-content errors:

1. **Quick fix for demos:** open the Vercel site and allow insecure content for that tab (Chrome: shield icon → site settings), **or**
2. **Better fix (still no custom domain):** use Hostinger’s free hostname / Cloudflare Tunnel / optional nginx+certbot later (see `deploy/nginx-ai-cargo.conf`)

For class demos, IP + port is usually enough once the firewall is open.

---

## 6. Cut over from Railway

1. `curl http://YOUR_VPS_IP:8000/api/windows?limit=1` works
2. Vercel redeployed with new `VITE_API_URL`
3. Click Overview + Agent Activity on the live site
4. Only then stop Railway

---

## Day-2 commands

```bash
cd /opt/ai-cargo
docker compose logs -f api
docker compose restart api
docker compose down
docker compose up -d --build   # after code updates
```

If Pathwise already uses port **8000**, change `docker-compose.yml` to:

```yaml
ports:
  - "8001:8000"
```

Then Vercel URL becomes `http://YOUR_VPS_IP:8001`.

---

## Sync + push yourself (when YOU are ready)

Cursor did **not** commit. When you want these files on your fork:

```bash
cd "/Users/rahul/Downloads/Code scripts/AI_cargo"

# you are on main, already synced with GitHub tip 596a34e
git status

git add Dockerfile docker-compose.yml requirements.docker.txt \
  .dockerignore .env.example HOSTINGER_DEPLOY.md deploy/ \
  .gitignore backend/app.py
# optional: deep-research-report.md

git commit -m "Add Hostinger Docker deploy for backend (Pathwise-style IP hosting)"

# push to YOUR fork
git push mine main

# if teammates use nsumesh repo and you have rights:
# git push origin main
```

---

## Architecture (Pathwise-identical)

```text
Browser
  └─ React (Vercel)   VITE_API_URL=http://VPS_IP:8000
        │
        ▼
   Hostinger VPS :8000  (Docker · uvicorn · FastAPI)
        │
        ├─ Supabase
        ├─ Groq
        └─ Gmail / Slack
```

No nginx. No domain. Same idea as Pathwise.
