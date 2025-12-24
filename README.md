# Bot nhac nho thanh toan Telegram

Bot Telegram nhac nho thanh toan hoa don. Du lieu duoc luu tren Google Sheets de tranh mat du lieu tren he thong khong luu tru.

## Yeu cau

- Python 3.10+ (khuyen nghi)
- Telegram Bot Token (tao tu BotFather)
- Google Sheets + Service Account

## Cai dat nhanh (local)

### 1) Tao Google Sheet

1. Tao mot Google Sheet ten `reminders`.
2. Tao 3 worksheet (tab) dung ten:
   - `Reminders`
   - `Users`
   - `Logs`
3. Them dong header (dong 1) cho moi tab:
   - Reminders: `id`, `user_id`, `text`, `day`, `time`, `frequency`, `timezone`, `active`, `created_at`, `last_sent`
   - Users: `id`, `name`, `created_at`, `timezone`
   - Logs: `reminder_id`, `sent_at`, `user_id`
4. Chia se sheet cho email cua Service Account.

### 2) Tao Service Account va credentials

1. Tao Service Account tren Google Cloud, bat API Google Sheets va Google Drive.
2. Tao key JSON.
3. Chon mot trong hai cach:
   - **Cach A (khuyen nghi):** dat bien moi truong `GOOGLE_CREDENTIALS_JSON` la toan bo noi dung JSON (mot dong).
   - **Cach B:** luu file `google_credentials.json` ngay trong thu muc goc du an.

### 3) Cau hinh bien moi truong

Tao file `.env` (khong commit):

```env
TELEGRAM_BOT_TOKEN=123456:ABCDEF_your_token_here
GOOGLE_CREDENTIALS_JSON={"type":"service_account","project_id":"..."}

# Tuy chon:
DEBUG_ALWAYS_ON=1
WINDOW_ONLY=0
```

Ghi chu:
- Neu ban dung file `google_credentials.json` thi khong can `GOOGLE_CREDENTIALS_JSON`.
- `DEBUG_ALWAYS_ON=1` se luon bat bot (bo qua khung gio hoat dong).
- `WINDOW_ONLY=1` se chi chay trong khung gio hoat dong roi thoat.

### 4) Cai dat thu vien va chay bot

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python bot.py
```

## Lenh su dung

- `/start` - Bat dau va dang ky nguoi dung
- `/add` - Tao loi nhac moi
- `/list` - Xem danh sach loi nhac
- `/remove` - Xoa loi nhac theo ID
- `/set_timezone` - Dat mui gio (vi du: `/set_timezone Asia/Ho_Chi_Minh`)
- `/help` - Huong dan chi tiet
- `/menu` - Menu nhanh

## Luu y ve lich hoat dong

Mac dinh bot co khung gio hoat dong 07:30-07:40 (gio VN) va nhac theo gio 07:35. Neu can bot luon hoat dong, bat `DEBUG_ALWAYS_ON=1`.

## Deploy len Heroku (tuy chon)

1. Tao app:

```bash
heroku create
```

2. Dat config vars tren Heroku (UI hoac CLI):
   - `TELEGRAM_BOT_TOKEN`
   - `GOOGLE_CREDENTIALS_JSON`
   - `DEBUG_ALWAYS_ON=1` (khuyen nghi)

3. Deploy:

```bash
git push heroku main
```

4. Chay dyno:

```bash
heroku ps:scale web=1
```

Neu Heroku bao loi khong bind `$PORT`, hay doi `Procfile` thanh `worker: python bot.py` va scale `worker=1` thay vi `web`.
