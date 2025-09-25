import os, sys, csv, subprocess

try:
    import pymysql  
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pymysql"])
    import pymysql  

import boto3

def need(name, cast=str):
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[ERROR] Falta variable de entorno {name}", file=sys.stderr); sys.exit(2)
    return cast(v)

HOST   = need("MYSQL_HOST")
PORT   = need("MYSQL_PORT", int)
DB     = need("MYSQL_DB")
USER   = need("MYSQL_USER")
PASS   = need("MYSQL_PASSWORD")
TABLE  = need("MYSQL_TABLE")

BUCKET = os.getenv("S3_BUCKET", "gcr-output-01")
KEY    = os.getenv("S3_KEY", "data.csv")  

LOCAL  = os.getenv("LOCAL_CSV", "data")

print(f"[INFO] MySQL {USER}@{HOST}:{PORT}/{DB} tabla={TABLE}")
conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASS, database=DB,
                       cursorclass=pymysql.cursors.DictCursor)
try:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM `{TABLE}`;")
        rows = cur.fetchall()
        if rows:
            headers = list(rows[0].keys())
        else:
            cur.execute(f"SHOW COLUMNS FROM `{TABLE}`;")
            headers = [r["Field"] for r in cur.fetchall()]

    with open(LOCAL, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow([r.get(h) for h in headers])

    print(f"[OK] CSV escrito: {LOCAL} (filas={len(rows)})")
finally:
    conn.close()

print(f"[INFO] Subiendo {LOCAL} a s3://{BUCKET}/{KEY}")
s3 = boto3.client("s3")
s3.upload_file(LOCAL, BUCKET, KEY)

print("Ingesta completada")
