"""
rbac.py  —  Role-Based Access Control for the kiosk application
================================================================
"""

from __future__ import annotations

import json
import functools
from datetime import datetime
from flask import (
    Blueprint, request, jsonify, session, abort, g, redirect
)

ROLE_SUPER_ADMIN = "Super Admin"
ROLE_ADMIN       = "Admin"
ROLE_USER        = "User"

ROLE_HIERARCHY = [ROLE_USER, ROLE_ADMIN, ROLE_SUPER_ADMIN]

ALL_PAGES = [
    # Parent group pages
    "user_page",
    "user_data_page",
    "config_page",
    "shift_config_page",
    # Child pages — User Page
    "register",
    "delete",
    # Child pages — User Data Page
    "import",
    "export",
    "logs",
    # Child pages — Config Page
    "userconfig",
    "device_config",
    "device_console",
    # Child pages — Shift Config Page
    "shift_master",
    "time_slot_master",
    "menu_master",
    "item_master",
    "item_limits",
    # Standalone pages (not grouped)
    "settings",
    "diagnostic",
    "discover",
    "modes",
    "permissions",
    "Network",
    "check_menu",
    # Biometric sub-pages (hidden from permissions UI, inherited from user_page)
    "edit",
    "finger_register",
    "finger_edit",
    "finger_delete",
    "rfid_register",
    "rfid_edit",
    "rfid_delete",
]

# ---------------------------------------------------------------------------
# Parent → Children grouping
# ---------------------------------------------------------------------------
PAGE_GROUPS = {
    "user_page":         ["register", "delete",
                          "edit",
                          "finger_register", "finger_edit", "finger_delete",
                          "rfid_register", "rfid_edit", "rfid_delete"],
    "user_data_page":    ["import", "export", "logs"],
    "config_page":       ["userconfig", "device_config", "device_console"],
    "shift_config_page": ["shift_master", "time_slot_master", "menu_master",
                          "item_master", "item_limits"],
}

CHILD_TO_PARENT = {}
for _parent, _children in PAGE_GROUPS.items():
    for _child in _children:
        CHILD_TO_PARENT[_child] = _parent

SUPER_ADMIN_PAGES = set(ALL_PAGES)

DEFAULT_ADMIN_PAGES = {
    "user_page", "user_data_page", "config_page", "shift_config_page",
    "register", "edit", "delete", "logs", "import", "export",
    "finger_register", "finger_edit", "finger_delete",
    "rfid_register", "rfid_edit", "rfid_delete",
    "userconfig", "device_config", "device_console",
    "shift_master", "time_slot_master",
    "menu_master", "item_master", "item_limits",
    "settings", "diagnostic", "discover", "modes", "permissions",
    "Network", "check_menu",
}

DEFAULT_USER_PAGES = {
    "check_menu",
}

_get_db = None
_app = None


def init_rbac(app, get_db_connection_func):
    global _get_db, _app
    _app = app
    _get_db = get_db_connection_func
    _ensure_tables()


def get_rbac():
    import rbac as _rbac
    return _rbac


def _db():
    if _get_db is None:
        raise RuntimeError("rbac.init_rbac() has not been called.")
    return _get_db()


def _ensure_tables():
    conn = _db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rbac_role_permissions (
            role        TEXT NOT NULL,
            page        TEXT NOT NULL,
            granted     INTEGER NOT NULL DEFAULT 1,
            updated_at  TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (role, page)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rbac_user_permissions (
            emp_id      TEXT NOT NULL,
            page        TEXT NOT NULL,
            granted     INTEGER NOT NULL DEFAULT 1,
            updated_at  TEXT DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (emp_id, page)
        )
    """)
    conn.commit()
    _seed_defaults(conn)


def _seed_defaults(conn):
    row = conn.execute("SELECT COUNT(*) FROM rbac_role_permissions").fetchone()
    if row[0] > 0:
        _ensure_new_pages(conn)
        return

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for page in ALL_PAGES:
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,1,?)",
            (ROLE_SUPER_ADMIN, page, now))

    for page in ALL_PAGES:
        granted = 1 if page in DEFAULT_ADMIN_PAGES else 0
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,?,?)",
            (ROLE_ADMIN, page, granted, now))

    for page in ALL_PAGES:
        granted = 1 if page in DEFAULT_USER_PAGES else 0
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,?,?)",
            (ROLE_USER, page, granted, now))

    conn.commit()


def _ensure_new_pages(conn):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    existing = set()
    for r in conn.execute("SELECT DISTINCT page FROM rbac_role_permissions").fetchall():
        existing.add(r[0])

    new_pages = [p for p in ALL_PAGES if p not in existing]
    if not new_pages:
        return

    for page in new_pages:
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,1,?)",
            (ROLE_SUPER_ADMIN, page, now))
        granted_admin = 1 if page in DEFAULT_ADMIN_PAGES else 0
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,?,?)",
            (ROLE_ADMIN, page, granted_admin, now))
        granted_user = 1 if page in DEFAULT_USER_PAGES else 0
        conn.execute(
            "INSERT OR IGNORE INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,?,?)",
            (ROLE_USER, page, granted_user, now))
        print(f"[RBAC] Added new page '{page}' to role permissions")

    conn.commit()


def get_role_permissions(role):
    conn = _db()
    rows = conn.execute(
        "SELECT page, granted FROM rbac_role_permissions WHERE role=?", (role,)
    ).fetchall()
    result = {p: False for p in ALL_PAGES}
    for r in rows:
        result[r["page"]] = bool(r["granted"])
    if _normalize_role(role) == ROLE_SUPER_ADMIN:
        result = {p: True for p in ALL_PAGES}
    return result


def get_user_allowed_pages(emp_id, role):
    role_norm = _normalize_role(role)
    if role_norm == ROLE_SUPER_ADMIN:
        return SUPER_ADMIN_PAGES.copy()

    role_perms = get_role_permissions(role_norm)
    allowed = {p for p, v in role_perms.items() if v}
    denied  = {p for p, v in role_perms.items() if not v}

    conn = _db()
    rows = conn.execute(
        "SELECT page, granted FROM rbac_user_permissions WHERE emp_id=?", (emp_id,)
    ).fetchall()
    for r in rows:
        if bool(r["granted"]):
            allowed.add(r["page"])
            denied.discard(r["page"])
        else:
            allowed.discard(r["page"])
            denied.add(r["page"])

    # ── Parent → Child expansion ──────────────────────────────────────────
    # If parent is granted, grant children UNLESS they are explicitly denied.
    # This ensures: user_page=1 + delete=0 → delete stays hidden.
    for parent, children in PAGE_GROUPS.items():
        if parent in allowed:
            for child in children:
                if child not in denied:
                    allowed.add(child)

    # ── Auto-grant parent visibility if ANY child is granted ──────────────
    # (parent page becomes accessible so user can navigate to sub-menu)
    for parent, children in PAGE_GROUPS.items():
        if parent not in allowed and any(c in allowed for c in children):
            allowed.add(parent)

    return allowed


def can_access_page(emp_id, role, page):
    return page in get_user_allowed_pages(emp_id, role)


def _normalize_role(role):
    if not role:
        return ROLE_USER
    r = role.strip().lower().replace(" ", "")
    if r == "superadmin":
        return ROLE_SUPER_ADMIN
    if r == "admin":
        return ROLE_ADMIN
    return ROLE_USER


def role_rank(role):
    n = _normalize_role(role)
    try:
        return ROLE_HIERARCHY.index(n)
    except ValueError:
        return 0


def can_modify_role(actor_role, target_role):
    return role_rank(actor_role) > role_rank(target_role)


def can_promote_to(actor_role, new_role):
    return role_rank(actor_role) > role_rank(new_role)


def can_assign_permissions_for(actor_role, target_role):
    return role_rank(actor_role) > role_rank(target_role)


def get_session_user():
    if not session.get("admin_session_active"):
        return None
    emp_id = session.get("admin_emp_id", "")
    if not emp_id:
        return None
    conn = _db()
    row = conn.execute(
        "SELECT emp_id, name, role FROM users WHERE emp_id=?", (emp_id,)
    ).fetchone()
    if not row:
        return {"emp_id": emp_id, "name": "Administrator", "role": ROLE_SUPER_ADMIN}
    return {"emp_id": row["emp_id"], "name": row["name"], "role": row["role"] or ROLE_USER}


def require_login(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not get_session_user():
            return jsonify({"success": False, "message": "Login required", "redirect": "/menu"}), 401
        return f(*args, **kwargs)
    return decorated


def require_permission(page):
    """For API endpoints — returns JSON 401/403 on failure."""
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            user = get_session_user()
            if not user:
                return jsonify({"success": False, "message": "Login required", "redirect": "/menu"}), 401
            if not can_access_page(user["emp_id"], user["role"], page):
                return jsonify({
                    "success": False,
                    "message": f"Access denied: you do not have permission for '{page}'.",
                    "page": page
                }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


def require_page_permission(page):
    """
    For HTML page routes — redirects to /menu on failure instead of
    returning JSON.  Use this on routes that render templates.
    """
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            user = get_session_user()
            if not user:
                return redirect("/menu")
            if not can_access_page(user["emp_id"], user["role"], page):
                return redirect("/menu")
            return f(*args, **kwargs)
        return decorated
    return decorator


def require_role(min_role):
    def decorator(f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            user = get_session_user()
            if not user:
                return jsonify({"success": False, "message": "Login required", "redirect": "/menu"}), 401
            if role_rank(user["role"]) < role_rank(min_role):
                return jsonify({
                    "success": False,
                    "message": f"Access denied: requires '{min_role}' or higher."
                }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


rbac_bp = Blueprint("rbac", __name__, url_prefix="/api/rbac")


@rbac_bp.route("/my_permissions", methods=["GET"])
def api_my_permissions():
    user = get_session_user()
    if not user:
        return jsonify({"success": False, "logged_in": False, "pages": [], "role": None}), 200
    pages = sorted(get_user_allowed_pages(user["emp_id"], user["role"]))
    return jsonify({
        "success":   True,
        "logged_in": True,
        "emp_id":    user["emp_id"],
        "name":      user["name"],
        "role":      user["role"],
        "pages":     pages,
        "is_super_admin": _normalize_role(user["role"]) == ROLE_SUPER_ADMIN,
        "is_admin":       _normalize_role(user["role"]) in (ROLE_ADMIN, ROLE_SUPER_ADMIN),
        "page_groups":    PAGE_GROUPS,
    })


@rbac_bp.route("/role_permissions", methods=["GET"])
@require_login
def api_get_role_permissions():
    actor = get_session_user()
    target_role = request.args.get("role", "").strip()
    if not target_role:
        return jsonify({"success": False, "message": "role param required"}), 400
    if not can_assign_permissions_for(actor["role"], target_role) \
       and _normalize_role(actor["role"]) != ROLE_SUPER_ADMIN:
        return jsonify({"success": False, "message": "Access denied"}), 403

    perms = get_role_permissions(target_role)
    return jsonify({"success": True, "role": target_role, "permissions": perms, "all_pages": ALL_PAGES})


@rbac_bp.route("/role_permissions", methods=["POST"])
@require_login
def api_set_role_permissions():
    actor = get_session_user()
    data  = request.get_json(force=True) or {}
    target_role  = (data.get("role") or "").strip()
    permissions  = data.get("permissions") or {}

    if not target_role:
        return jsonify({"success": False, "message": "role required"}), 400
    if not can_assign_permissions_for(actor["role"], target_role):
        return jsonify({"success": False, "message": "You cannot modify permissions for this role."}), 403
    if _normalize_role(target_role) == ROLE_SUPER_ADMIN:
        return jsonify({"success": False, "message": "Super Admin permissions cannot be modified."}), 403

    now  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = _db()
    for page in ALL_PAGES:
        granted = 1 if permissions.get(page) else 0
        conn.execute(
            """INSERT INTO rbac_role_permissions(role,page,granted,updated_at) VALUES(?,?,?,?)
               ON CONFLICT(role,page) DO UPDATE SET granted=excluded.granted, updated_at=excluded.updated_at""",
            (target_role, page, granted, now))
    conn.commit()

    return jsonify({"success": True, "message": f"Permissions updated for role '{target_role}'."})


@rbac_bp.route("/users", methods=["GET"])
@require_login
def api_rbac_users():
    actor = get_session_user()
    actor_rank = role_rank(actor["role"])

    conn = _db()
    rows = conn.execute(
        "SELECT emp_id, name, role FROM users WHERE emp_id IS NOT NULL AND emp_id != '' ORDER BY emp_id"
    ).fetchall()

    result = []
    for r in rows:
        r_role = r["role"] or ROLE_USER
        if role_rank(r_role) < actor_rank:
            result.append({
                "emp_id": r["emp_id"],
                "name":   r["name"] or "",
                "role":   r_role,
                "can_promote": _get_promotable_roles(actor["role"], r_role),
            })

    return jsonify({"success": True, "users": result})


def _get_promotable_roles(actor_role, current_target_role):
    actor_rank_val  = role_rank(actor_role)
    target_rank_val = role_rank(current_target_role)
    promotable = []
    for role in ROLE_HIERARCHY:
        r_rank = role_rank(role)
        if target_rank_val < r_rank < actor_rank_val:
            promotable.append(role)
    return promotable


@rbac_bp.route("/set_role", methods=["POST"])
@require_login
def api_set_role():
    actor = get_session_user()
    data  = request.get_json(force=True) or {}
    emp_id   = (data.get("emp_id") or "").strip()
    new_role = (data.get("new_role") or "").strip()

    if not emp_id or not new_role:
        return jsonify({"success": False, "message": "emp_id and new_role required"}), 400

    conn = _db()
    row  = conn.execute("SELECT emp_id, name, role FROM users WHERE emp_id=?", (emp_id,)).fetchone()
    if not row:
        return jsonify({"success": False, "message": "User not found"}), 404

    current_role = row["role"] or ROLE_USER

    if emp_id == actor["emp_id"]:
        return jsonify({"success": False, "message": "You cannot modify your own role."}), 403
    if not can_modify_role(actor["role"], current_role):
        return jsonify({"success": False, "message": "You cannot modify a user with equal or higher role."}), 403
    if not can_promote_to(actor["role"], new_role):
        return jsonify({"success": False, "message": "You cannot assign a role equal to or higher than your own."}), 403

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn.execute("UPDATE users SET role=?, updated_at=? WHERE emp_id=?", (new_role, now, emp_id))
    conn.commit()

    return jsonify({
        "success": True,
        "message": f"Role of {row['name'] or emp_id} updated to '{new_role}'.",
        "emp_id": emp_id, "old_role": current_role, "new_role": new_role,
    })


@rbac_bp.route("/check_page", methods=["GET"])
def api_check_page():
    page = (request.args.get("page") or "").strip()
    user = get_session_user()
    if not user:
        return jsonify({"allowed": False, "reason": "not_logged_in"}), 200
    allowed = can_access_page(user["emp_id"], user["role"], page)
    return jsonify({"allowed": allowed, "role": user["role"], "emp_id": user["emp_id"], "page": page}), 200


@rbac_bp.route("/all_pages", methods=["GET"])
def api_all_pages():
    return jsonify({"success": True, "pages": ALL_PAGES})
