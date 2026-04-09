"""
firestore_storage.py — Persistencia en Firebase Firestore para el curador.
Reemplaza los archivos JSON locales que se perdían al redeploy en Render.

Colección Firestore: curator/
  - curator/session → sesión de curación (accepted, rejected, current_index)
  - curator/brands_{country} → marcas activas por país
  - curator/cache_{country} → cache de productos crawleados
  - curator/config → país activo y settings
"""

import json
import os

# Intentar inicializar Firebase Admin
_db = None
_initialized = False

def _init_firebase():
    global _db, _initialized
    if _initialized:
        return _db is not None
    _initialized = True
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore

        # Opción 1: Service account JSON desde variable de entorno
        sa_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
        if sa_json:
            sa_dict = json.loads(sa_json)
            cred = credentials.Certificate(sa_dict)
            firebase_admin.initialize_app(cred)
        else:
            # Opción 2: Solo project ID (funciona en algunos entornos)
            project_id = os.environ.get("FIREBASE_PROJECT_ID", "mode-app-4f7cd")
            try:
                firebase_admin.initialize_app(options={"projectId": project_id})
            except Exception:
                print("⚠️ Firebase Admin no configurado. Usando archivos locales como fallback.")
                return False

        _db = firestore.client()
        print("✅ Firebase Firestore conectado para persistencia del curador")
        return True
    except Exception as e:
        print(f"⚠️ Firebase init error: {e}. Usando archivos locales como fallback.")
        return False


def _get_db():
    if not _initialized:
        _init_firebase()
    return _db


# ── Session ──────────────────────────────────────────────────────────────

def save_session_firestore(session_data, user_id="default"):
    """Guarda sesión de curación en Firestore."""
    db = _get_db()
    if not db:
        return False
    try:
        # Guardar metadata en doc principal (sin listas grandes)
        accepted = session_data.get("accepted", [])
        rejected = session_data.get("rejected", [])
        data = {
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "current_index": session_data.get("current_index", 0),
            "previous_urls": session_data.get("previous_urls", [])[:1000],
            "updated_at": firestore_timestamp(),
        }
        db.collection("curator").document(f"session_{user_id}").set(data)

        # Helper: write chunks and clean orphans up to a safe max index
        def _write_chunks(prefix, items, chunk_size, key="products"):
            num_chunks = (len(items) + chunk_size - 1) // chunk_size if items else 0
            written_indices = set()
            for i in range(0, max(len(items), 1), chunk_size):
                idx = i // chunk_size
                doc_id = f"{user_id}_{prefix}_{idx}"
                db.collection("curator").document(doc_id).set({
                    key: items[i:i + chunk_size], "chunk_index": idx,
                })
                written_indices.add(idx)
            # Clean orphans: check indices beyond what we wrote, up to 100
            # This handles cases where the list shrunk from a large to small size
            for idx in range(num_chunks, num_chunks + 100):
                doc_id = f"{user_id}_{prefix}_{idx}"
                doc = db.collection("curator").document(doc_id).get()
                if doc.exists:
                    doc.reference.delete()
                else:
                    break  # No more orphans beyond this point

        _write_chunks("accepted", accepted, 100, "products")
        _write_chunks("rejected", rejected, 500, "urls")

        prev_rows = session_data.get("previous_rows", [])
        _write_chunks("rows", prev_rows, 200, "rows")
        return True
    except Exception as e:
        print(f"Error guardando sesión en Firestore: {e}")
        return False


def load_session_firestore(user_id="default"):
    """Carga sesión de curación desde Firestore."""
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("curator").document(f"session_{user_id}").get()
        if not doc.exists:
            return None
        data = doc.to_dict()

        # Cargar accepted de chunks
        accepted = []
        chunk_idx = 0
        while True:
            chunk_doc = db.collection("curator").document(f"{user_id}_accepted_{chunk_idx}").get()
            if not chunk_doc.exists:
                break
            accepted.extend(chunk_doc.to_dict().get("products", []))
            chunk_idx += 1
        data["accepted"] = accepted

        # Cargar rejected de chunks
        rejected = []
        chunk_idx = 0
        while True:
            chunk_doc = db.collection("curator").document(f"{user_id}_rejected_{chunk_idx}").get()
            if not chunk_doc.exists:
                break
            rejected.extend(chunk_doc.to_dict().get("urls", []))
            chunk_idx += 1
        data["rejected"] = rejected

        # Cargar previous_rows de chunks
        prev_rows = []
        chunk_idx = 0
        while True:
            chunk_doc = db.collection("curator").document(f"{user_id}_rows_{chunk_idx}").get()
            if not chunk_doc.exists:
                break
            prev_rows.extend(chunk_doc.to_dict().get("rows", []))
            chunk_idx += 1
        if prev_rows:
            data["previous_rows"] = prev_rows

        return data
    except Exception as e:
        print(f"Error cargando sesión de Firestore: {e}")
        return None


# ── Brands ───────────────────────────────────────────────────────────────

def save_brands_firestore(brands, country):
    """Guarda marcas activas para un país."""
    db = _get_db()
    if not db:
        return False
    try:
        db.collection("curator").document(f"brands_{country}").set({
            "brands": brands,
            "country": country,
            "updated_at": firestore_timestamp(),
        })
        return True
    except Exception as e:
        print(f"Error guardando brands en Firestore: {e}")
        return False


def load_brands_firestore(country):
    """Carga marcas activas para un país."""
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("curator").document(f"brands_{country}").get()
        if not doc.exists:
            return None
        return doc.to_dict().get("brands", [])
    except Exception as e:
        print(f"Error cargando brands de Firestore: {e}")
        return None


# ── Country ──────────────────────────────────────────────────────────────

def save_country_firestore(country):
    db = _get_db()
    if not db:
        return False
    try:
        db.collection("curator").document("config").set({"active_country": country}, merge=True)
        return True
    except Exception as e:
        return False


def load_country_firestore():
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("curator").document("config").get()
        if not doc.exists:
            return None
        return doc.to_dict().get("active_country", "")
    except Exception as e:
        return None


# ── Cache ────────────────────────────────────────────────────────────────

def save_cache_firestore(products, country):
    """Guarda cache de productos crawleados. Puede ser grande — dividir en chunks."""
    db = _get_db()
    if not db:
        return False
    try:
        # Guardar metadata
        db.collection("curator").document(f"cache_{country}").set({
            "product_count": len(products),
            "country": country,
            "updated_at": firestore_timestamp(),
        })
        # Guardar productos en chunks de 100
        # Batch: delete old + write new in one operation (prevents race conditions)
        old_chunks = list(db.collection("curator").document(f"cache_{country}").collection("chunks").stream())
        batch = db.batch()
        for old in old_chunks:
            batch.delete(old.reference)
        # Commit deletes first (batch limit is 500)
        if old_chunks:
            batch.commit()

        for i in range(0, len(products), 100):
            # Trim products to avoid 1MB Firestore document limit
            chunk = []
            for p in products[i:i+100]:
                trimmed = dict(p)
                if trimmed.get("description") and len(str(trimmed["description"])) > 300:
                    trimmed["description"] = str(trimmed["description"])[:300] + "..."
                if trimmed.get("all_images") and len(trimmed["all_images"]) > 5:
                    trimmed["all_images"] = trimmed["all_images"][:5]
                chunk.append(trimmed)
            db.collection("curator").document(f"cache_{country}").collection("chunks").document(f"chunk_{i//100}").set({
                "products": chunk,
                "index": i//100,
            })
        return True
    except Exception as e:
        print(f"Error guardando cache en Firestore: {e}")
        return False


def load_cache_firestore(country):
    """Carga cache de productos desde Firestore."""
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("curator").document(f"cache_{country}").get()
        if not doc.exists:
            return None

        products = []
        chunks = db.collection("curator").document(f"cache_{country}").collection("chunks").order_by("index").stream()
        for chunk_doc in chunks:
            products.extend(chunk_doc.to_dict().get("products", []))
        return products
    except Exception as e:
        print(f"Error cargando cache de Firestore: {e}")
        return None


# ── Clear ────────────────────────────────────────────────────────────────

def clear_session_firestore(user_id="default"):
    """Limpia sesión de curación — delete by known doc IDs (no collection scan)."""
    db = _get_db()
    if not db:
        return False
    try:
        col = db.collection("curator")
        batch = db.batch()
        count = 0
        # Delete main session doc
        batch.delete(col.document(f"session_{user_id}"))
        count += 1
        # Delete chunk docs by index (check existence, stop at first gap)
        for prefix in ["accepted", "rejected", "rows"]:
            for idx in range(100):  # Max 100 chunks per type
                doc_id = f"{user_id}_{prefix}_{idx}"
                doc = col.document(doc_id).get()
                if doc.exists:
                    batch.delete(col.document(doc_id))
                    count += 1
                    if count >= 490:  # Flush batch before hitting 500 limit
                        batch.commit()
                        batch = db.batch()
                        count = 0
                else:
                    break  # No more chunks
        # Also delete hidden brands doc
        batch.delete(col.document(f"hidden_{user_id}"))
        batch.commit()
        return True
    except Exception as e:
        print(f"Error clearing session Firestore: {e}")
        return False


def clear_cache_firestore(country):
    """Limpia cache de un país — batch delete."""
    db = _get_db()
    if not db:
        return False
    try:
        doc_ref = db.collection("curator").document(f"cache_{country}")
        chunks = list(doc_ref.collection("chunks").stream())
        if chunks:
            batch = db.batch()
            for chunk in chunks:
                batch.delete(chunk.reference)
            batch.delete(doc_ref)
            batch.commit()
        else:
            doc_ref.delete()
        return True
    except Exception as e:
        return False


def clear_all_firestore():
    """Limpia toda la data del curador en Firestore."""
    db = _get_db()
    if not db:
        return False
    try:
        docs = db.collection("curator").stream()
        for doc in docs:
            # Limpiar subcolecciones de chunks
            for sub in doc.reference.collections():
                for sub_doc in sub.stream():
                    sub_doc.reference.delete()
            doc.reference.delete()
        return True
    except Exception as e:
        return False


# ── Helper ───────────────────────────────────────────────────────────────

def firestore_timestamp():
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def is_firestore_available():
    """Verifica si Firestore está disponible."""
    return _get_db() is not None
