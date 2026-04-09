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

        # Guardar accepted en chunks (sin límite de 500)
        num_acc_chunks = max((len(accepted) + 99) // 100, 1)
        for i in range(0, max(len(accepted), 1), 100):
            chunk = accepted[i:i+100]
            db.collection("curator").document(f"{user_id}_accepted_{i//100}").set({
                "products": chunk, "chunk_index": i//100,
            })
        # Limpiar chunks sobrantes (start from first unused chunk index)
        for i in range(num_acc_chunks, num_acc_chunks + 15):
            try: db.collection("curator").document(f"{user_id}_accepted_{i}").delete()
            except Exception: pass

        # Guardar rejected en chunks
        num_rej_chunks = max((len(rejected) + 499) // 500, 1)
        for i in range(0, max(len(rejected), 1), 500):
            chunk = rejected[i:i+500]
            db.collection("curator").document(f"{user_id}_rejected_{i//500}").set({
                "urls": chunk, "chunk_index": i//500,
            })
        # Cleanup orphaned rejected chunks
        for i in range(num_rej_chunks, num_rej_chunks + 10):
            try: db.collection("curator").document(f"{user_id}_rejected_{i}").delete()
            except Exception: pass

        # Guardar previous_rows en chunks
        prev_rows = session_data.get("previous_rows", [])
        if prev_rows:
            for i in range(0, len(prev_rows), 200):
                chunk = prev_rows[i:i+200]
                db.collection("curator").document(f"{user_id}_rows_{i//200}").set({
                    "rows": chunk, "chunk_index": i//200,
                })
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
        # Guardar productos en chunks de 100 (cada producto puede ser grande)
        # Primero limpiar chunks anteriores
        old_chunks = db.collection("curator").document(f"cache_{country}").collection("chunks").stream()
        for old in old_chunks:
            old.reference.delete()

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
    """Limpia sesión de curación — query-based delete (no fixed ranges)."""
    db = _get_db()
    if not db:
        return False
    try:
        col = db.collection("curator")
        # Find ALL documents belonging to this user by prefix query
        prefixes = [f"session_{user_id}", f"{user_id}_accepted_",
                    f"{user_id}_rejected_", f"{user_id}_rows_"]
        docs_to_delete = []
        for doc in col.stream():
            doc_id = doc.id
            for prefix in prefixes:
                if doc_id == prefix or doc_id.startswith(prefix):
                    docs_to_delete.append(doc.reference)
                    break
        if not docs_to_delete:
            return True
        # Batch delete in groups of 500 (Firestore batch limit)
        for i in range(0, len(docs_to_delete), 500):
            batch = db.batch()
            for ref in docs_to_delete[i:i+500]:
                batch.delete(ref)
            batch.commit()
        print(f"Cleared {len(docs_to_delete)} Firestore docs for user {user_id}")
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
