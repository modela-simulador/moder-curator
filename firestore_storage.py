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

def save_session_firestore(session_data):
    """Guarda sesión de curación en Firestore."""
    db = _get_db()
    if not db:
        return False
    try:
        # Firestore tiene límite de 1MB por documento
        # Si la sesión es muy grande, guardar solo metadata
        data = {
            "accepted": session_data.get("accepted", [])[:500],  # Limitar para no exceder 1MB
            "rejected": session_data.get("rejected", [])[:2000],
            "current_index": session_data.get("current_index", 0),
            "previous_urls": session_data.get("previous_urls", [])[:1000],
            "updated_at": firestore_timestamp(),
        }
        # previous_rows puede ser muy grande — guardar aparte si existe
        db.collection("curator").document("session").set(data)

        prev_rows = session_data.get("previous_rows", [])
        if prev_rows:
            # Guardar en chunks de 200 (límite Firestore)
            for i in range(0, len(prev_rows), 200):
                chunk = prev_rows[i:i+200]
                db.collection("curator").document(f"session_rows_{i//200}").set({
                    "rows": chunk,
                    "chunk_index": i//200,
                })
        return True
    except Exception as e:
        print(f"Error guardando sesión en Firestore: {e}")
        return False


def load_session_firestore():
    """Carga sesión de curación desde Firestore."""
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("curator").document("session").get()
        if not doc.exists:
            return None
        data = doc.to_dict()

        # Cargar previous_rows de chunks
        prev_rows = []
        chunk_idx = 0
        while True:
            chunk_doc = db.collection("curator").document(f"session_rows_{chunk_idx}").get()
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
            chunk = products[i:i+100]
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

def clear_session_firestore():
    """Limpia sesión de curación."""
    db = _get_db()
    if not db:
        return False
    try:
        db.collection("curator").document("session").delete()
        # Limpiar chunks de previous_rows
        for i in range(20):  # Max 20 chunks
            db.collection("curator").document(f"session_rows_{i}").delete()
        return True
    except Exception as e:
        return False


def clear_cache_firestore(country):
    """Limpia cache de un país."""
    db = _get_db()
    if not db:
        return False
    try:
        doc_ref = db.collection("curator").document(f"cache_{country}")
        # Limpiar chunks
        chunks = doc_ref.collection("chunks").stream()
        for chunk in chunks:
            chunk.reference.delete()
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
