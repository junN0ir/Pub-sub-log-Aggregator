# UTS Sistem Terdistribusi
# Junnior MArcellino Polla - 11231034

Proyek ini merupakan implementasi layanan **Pub-Sub Log Aggregator** berbasis Python dengan FastAPI dan asyncio. Sistem menerima event dari publisher, memprosesnya melalui internal queue, dan melakukan deduplication menggunakan **SQLite** sebagai persistent store agar tahan terhadap restart container.
Seluruh sistem berjalan lokal dalam **Docker**, tanpa koneksi layanan eksternal publik.

---

## Fitur Utama
- **Publish Event (POST /publish)**
  Menerima event JSON single maupun batch dan memasukkannya ke asyncio.Queue internal.
- **Deduplication & Idempotency**
  Event unik disimpan di SQLite berdasarkan primary key (topic, event_id). Duplikat diabaikan.
- **Persistence**
  Dedup store tetap efektif mencegah reprocessing setelah container di-restart.
- **Observability**
  Endpoint `/stats` menampilkan statistik lengkap sistem secara real-time.
- **Swagger UI**
  Dokumentasi API otomatis tersedia di `http://localhost:8080/docs`.

---

## Cara Build dan Run

### 1. Build Docker Image
Jalankan perintah berikut di terminal:
```powershell
docker build -t uts-aggregator .
```

### 2. Run Docker Container
```powershell
docker run -d --name aggregator-demo -p 8080:8080 -v aggregator-data:/app/data uts-aggregator
```
Setelah berhasil, cek log container:
```powershell
docker logs aggregator-demo
```
Akan muncul:

INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8080 (Press CTRL+C to quit)

---

## Asumsi Sistem
Kombinasi unik `(topic, event_id)` digunakan sebagai kunci deteksi duplikat.
<br>Semua komponen berjalan lokal dalam satu jaringan internal container.
<br>Delivery bersifat at-least-once — publisher diizinkan mengirim ulang event yang sama.
<br>Total ordering tidak diterapkan, partial ordering via timestamp sudah cukup.
<br>SQLite sebagai dedup store lokal menjamin persistensi dan idempotency.

---

## Endpoint API
Buka browser ke `http://localhost:8080/docs` untuk Swagger UI.

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/publish` | Kirim batch atau single event |
| GET | `/events?topic=...` | Lihat event unik per topic |
| GET | `/stats` | Statistik sistem |
| GET | `/health` | Health check |

---

## Demo Penggunaan

### Kirim Event
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/publish" -Method Post `
  -ContentType "application/json" -Body '{
    "events": [{
      "topic": "demo.logs",
      "event_id": "evt-001",
      "timestamp": "2025-01-01T10:00:00Z",
      "source": "demo_client",
      "payload": { "msg": "event pertama" }
    }]
  }'
```

### Kirim Event Duplikat (simulasi at-least-once)
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/publish" -Method Post `
  -ContentType "application/json" -Body '{
    "events": [{
      "topic": "demo.logs",
      "event_id": "evt-001",
      "timestamp": "2025-01-01T10:00:00Z",
      "source": "demo_client",
      "payload": { "msg": "duplikat, harus di-drop" }
    }]
  }'
```

### Cek Statistik
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/stats"
```

### Cek Event Tersimpan
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/events?topic=demo.logs"
```

### Restart Container — Uji Persistensi
```powershell
docker ps
docker restart aggregator-demo
Write-Host "Menunggu 5 detik setelah restart..."
Start-Sleep -Seconds 5
```
Setelah container aktif kembali, kirim ulang event yang sama:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/publish" -Method Post `
  -ContentType "application/json" -Body '{
    "events": [{
      "topic": "demo.logs",
      "event_id": "evt-001",
      "timestamp": "2025-01-01T10:00:00Z",
      "source": "demo_client",
      "payload": { "msg": "ulang setelah restart" }
    }]
  }'
```
Cek stats — `duplicate_dropped` bertambah, bukan `unique_processed`:
```powershell
Invoke-RestMethod -Uri "http://localhost:8080/stats"
```

### Stress Test 5000 Event (20% Duplikat)
```powershell
$events = @()

for ($i = 1; $i -le 5000; $i++) {
    $id = if ($i % 5 -eq 0) { "evt-$($i - 1)" } else { "evt-$i" }
    $events += @{
        topic     = "stress.logs"
        event_id  = $id
        timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ssZ")
        source    = "stress_tester"
        payload   = @{ msg = "Event $i" }
    }
}

$jsonBody = @{ events = $events } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri "http://localhost:8080/publish" -Method Post `
  -ContentType "application/json" -Body $jsonBody

Start-Sleep -Seconds 3
Invoke-RestMethod -Uri "http://localhost:8080/stats"
```

---

## Run Docker Compose (Bonus)
Menjalankan aggregator dan publisher sebagai dua service terpisah:
```powershell
docker-compose up --build
```

---

## Menjalankan Unit Tests
```powershell
py -3.11 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
pytest tests/ -v
```

---

## 🎥 Video Demo
▶[link youtube sedang dibuat]

---

*UTS Mata Kuliah Sistem Paralel dan Terdistribusi*