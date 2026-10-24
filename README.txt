RENDER DEPLOY
1. Is zip ki saari files GitHub repo ke ROOT me daalo
2. Render -> New -> Web Service -> repo chuno
   Build : pip install -r requirements.txt
   Start : uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
3. Self keep-alive apne aap RENDER_EXTERNAL_URL se chalta hai (har 7 min /health ping)

UPTIMEROBOT (extra backup, free)
1. uptimerobot.com par account banao
2. Add New Monitor -> type: HTTP(s)
3. URL: https://APNA-APP.onrender.com/health
4. Interval: 5 minutes -> Create

USE
/get?type=https&sec=0.1&count=5
/get?type=http&ms=500&count=10
/get?type=both&sec=2&count=20
/stats   (keepalive status bhi dikhta hai)
